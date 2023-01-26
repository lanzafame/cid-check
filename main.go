package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	bsnet "github.com/ipfs/go-bitswap/network"
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
	Cid       cid.Cid
	Duration  time.Duration
	Found     bool
	Responded bool
	Error     string
}

type FileMux struct {
	sync.Mutex
	f *os.File
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
	fmux := FileMux{f: f}

	progressF, err := os.OpenFile(fmt.Sprintf("cid-check.progress.%d", timestamp), os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Println(err)
		return err
	}
	defer progressF.Close()

	// debugF, err := os.OpenFile(fmt.Sprintf("cid-check.debug.%d", timestamp), os.O_CREATE|os.O_WRONLY, 0644)
	// if err != nil {
	// 	log.Println(err)
	// 	return err
	// }
	// defer debugF.Close()
	// dmux := FileMux{f: debugF}

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
	// for _, c := range cs {
	// 	fmt.Println(c)
	// }

	dialCtx, dialCancel := context.WithTimeout(ctx, time.Second*3)
	connErr := testHost.Connect(dialCtx, *ai)
	dialCancel()
	if connErr != nil {
		fmt.Println(connErr.Error())
		//TODO: retry logic
		return connErr
	}

	target := ai.ID
	rcv := &bsReceiver{
		target: target,
		result: make(chan msgOrErr),
	}

	bs := BS{bsnet.NewFromIpfsHost(testHost, nilRouter), rcv}

	bs.Start(rcv)
	defer bs.Stop()

	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(cctx.Int("goroutines"))

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			batchsize := 500
			for i := offset; i < len(cs); i = i + batchsize {
				var c []cid.Cid
				if i+batchsize > len(cs) {
					c = cs[i:]
				} else {
					c = cs[i : i+batchsize]
				}

				if _, err := progressF.WriteString(fmt.Sprintf("%d: %s; len: %d\n", i, c, len(c))); err != nil {
					fmt.Println("failed to write to progress file")
					fmt.Println(i)
				}

				if (i+1)%10 == 0 {
					percent := float64((i - offset)) / float64(len(cs)-offset-1) * float64(100)
					fmt.Printf("%d/%d\t%f%%\t%s\n", i-offset, len(cs)-offset-1, percent, cs[i])
				}

				g.Go(func() error {
					start := time.Now()
					c := c

					moe := bs.haveCIDs(ctx, c, *ai)
					if moe.err != nil {
						return moe.err
					}

					if len(moe.msg.DontHaves()) > 0 {
						// debugF.WriteString(fmt.Sprintf("donthaves: %d\n", len(moe.msg.DontHaves())))
						fmux.Lock()
						for _, msg := range moe.msg.DontHaves() {
							fmux.f.WriteString(fmt.Sprintf("%+v\n", msg))
						}
						fmux.Unlock()
					}

					fmt.Fprintf(
						os.Stdout,
						"haves: %d\tdonthaves: %d\tdur: %+v\n",
						len(moe.msg.Haves()), len(moe.msg.DontHaves()), time.Since(start),
					)

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
