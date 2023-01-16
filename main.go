package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/p2p/muxer/mplex"
	"github.com/libp2p/go-libp2p/p2p/muxer/yamux"
	"github.com/urfave/cli/v2"
	"golang.org/x/sync/errgroup"
)

func main() {
	app := &cli.App{
		Name:  "cid-check",
		Usage: "check cids",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name: "peer",
			},
			&cli.StringFlag{
				Name: "cid-file",
			},
			&cli.IntFlag{
				Name:    "goroutines",
				Aliases: []string{"gs"},
				Usage:   "the number of goroutines used to verify cids",
				Value:   5,
			},
			&cli.IntFlag{
				Name:    "offset",
				Aliases: []string{"o"},
				Usage:   "specify which line to start on in input file; note: 1-indexed",
				Value:   1,
			},
		},
		Action: check,
	}

	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}
}

type Output struct {
	Cid                      cid.Cid
	ConnectionError          string
	DataAvailableOverBitswap BsCheckOutput
}

type BsCheckOutput struct {
	Duration  time.Duration
	Found     bool
	Responded bool
	Error     string
}

func check(cctx *cli.Context) error {
	ctx := context.Background()

	timestamp := time.Now().Unix()

	f, err := os.OpenFile(fmt.Sprintf("cid-check.failed.cids.%d", timestamp), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Println(err)
		return err
	}
	defer f.Close()

	progressF, err := os.OpenFile(fmt.Sprintf("cid-check.progress.%d", timestamp), os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Println(err)
		return err
	}
	defer progressF.Close()

	offset := cctx.Int("offset") - 1
	if offset < 0 {
		offset = 0
	}

	// load cid file into memory before setting up ipfs host and peer connection

	// create ipfs host
	testHost, err := libp2p.New(libp2p.ChainOptions(libp2p.Muxer("/yamux/1.0.0", yamux.DefaultTransport), libp2p.Muxer("/mplex/6.7.0", mplex.DefaultTransport)), libp2p.ConnectionGater(&privateAddrFilterConnectionGater{}))
	if err != nil {
		return fmt.Errorf("server error: %w", err)
	}
	defer testHost.Close()

	// process peer multiaddr flag
	ai, err := peer.AddrInfoFromString(cctx.String("peer"))
	if err != nil {
		return err
	}

	cids, err := processCIDFile(cctx.String("cid-file"))
	if err != nil {
		return err
	}
	var cs []cid.Cid
	for _, cstr := range cids {
		c, err := cid.Decode(cstr)
		if err != nil {
			return err
		}
		cs = append(cs, c)
	}

	dialCtx, dialCancel := context.WithTimeout(ctx, time.Second*3)
	connErr := testHost.Connect(dialCtx, *ai)
	dialCancel()
	if connErr != nil {
		fmt.Println(connErr.Error())
		//TODO: retry logic
		return connErr
	}

	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(cctx.Int("goroutines"))

	// outputs := []*Output{}
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			for i := offset; i < len(cs); i++ {
				c := cs[i]

				if _, err := progressF.WriteString(fmt.Sprintln(i)); err != nil {
					fmt.Println("failed to write to progress file")
					fmt.Println(i)
				}

				g.Go(func() error {
					c := c

					bsOut := checkBitswapCID(ctx, testHost, c, *ai)
					if bsOut.Error != "" {
						return fmt.Errorf(bsOut.Error)
					}
					if !bsOut.Found {
						if _, err := f.WriteString(fmt.Sprintf("%s\n", c)); err != nil {
							fmt.Println("failed to write failed cid to file")
							fmt.Println(cs)
						}
						return nil
					} else {
						fmt.Printf("%s was found\n", c)
					}
					return nil
				})
			}

			if err := g.Wait(); err != nil {
				return fmt.Errorf("errgroup wait failed: %w", err)
			}
		}
	}
}

func processCIDFile(filename string) ([]string, error) {
	data, err := os.ReadFile(filename)
	if err != nil {
		log.Fatal(err)
		return nil, err
	}

	str := string(data)
	cidsstr := strings.Split(str, "\n")
	cidsstr = cidsstr[:len(cidsstr)-1]
	return cidsstr, nil
}
