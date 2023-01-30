package main

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/exec"
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
			&cli.BoolFlag{
				Name:    "dagexport",
				Aliases: []string{"d"},
				Value:   false,
			},
		},
		Action: check,
	}

	// server to access pprof stats
	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()
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

func check(cctx *cli.Context) error {
	ctx := context.Background()

	timestamp := time.Now().Unix()

	f, err := outputFile("cid-check.failed.cids", timestamp)
	if err != nil {
		log.Println(err)
		return err
	}
	defer f.Close()

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
		return connErr
	}

	target := ai.ID

	g, _ := errgroup.WithContext(ctx)
	g.SetLimit(cctx.Int("goroutines"))

	results := make(chan msgOrErr)

	p, _ := errgroup.WithContext(ctx)
	p.SetLimit(10)

	d, _ := errgroup.WithContext(ctx)
	d.SetLimit(500)

	dagexport := cctx.Bool("dagexport")
	go func() error {
		for {
			select {
			case m := <-results:
				p.Go(func() error {
					err := processMsg(m, f)
					if err != nil {
						return err
					}
					return nil
				})
				if dagexport {
					d.Go(func() error {
						if len(m.msg.DontHaves()) > 0 {
							for _, msg := range m.msg.DontHaves() {
								d.Go(func() error {
									msg := msg
									err := dagExport(msg.String())
									if err != nil {
										fmt.Fprintf(os.Stderr, "err: %s", err.Error())
									}
									return nil
								})
							}
						}
						return nil
					})
				}
			case <-ctx.Done():
				return nil
			}
		}
	}()

	batchsize := 500
	for i := offset; i < len(cs); i = i + batchsize {
		var c []cid.Cid
		if i+batchsize > len(cs) {
			c = cs[i:]
		} else {
			c = cs[i : i+batchsize]
		}

		g.Go(func() error {
			c := c

			ctx, cancel := context.WithTimeout(ctx, time.Second*5)
			defer cancel()
			err := haveCIDs(ctx, testHost, ai, target, c, results)
			if err != nil {
				return err
			}
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return fmt.Errorf("errgroup wait failed: %w", err)
	}
	fmt.Println("waiting for p group to return...")
	if err := p.Wait(); err != nil {
		return fmt.Errorf("errgroup wait failed: %w", err)
	}
	if err := d.Wait(); err != nil {
		return fmt.Errorf("errgroup wait failed: %w", err)
	}

	return nil
}

func processMsg(moe msgOrErr, f *os.File) error {
	dur := time.Since(moe.start)
	if moe.err != nil {
		fmt.Fprintf(os.Stderr, "processing error: %s\n", moe.err.Error())
		return nil
	}
	// fmt.Fprintf(os.Stdout, "moe: %+v\n", moe)

	if len(moe.msg.DontHaves()) > 0 {
		for _, msg := range moe.msg.DontHaves() {
			f.WriteString(fmt.Sprintf("%+v\n", msg))
		}
	}

	fmt.Fprintf(
		os.Stdout,
		"haves: %d\tdonthaves: %d\tdur: %v\n",
		len(moe.msg.Haves()), len(moe.msg.DontHaves()), dur,
	)
	return nil
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

func dagExport(cid string) error {
	cmd := exec.Command("ipfs", "dag", "export", cid)
	var out bytes.Buffer
	cmd.Stdout = &out
	err := cmd.Start()
	if err != nil {
		return err
	}
	err = cmd.Wait()
	if err != nil {
		return err
	}

	wd, err := os.Getwd()
	if err != nil {
		return err
	}

	f, err := os.Create(fmt.Sprintf("%s/%s.car", wd, cid))
	if err != nil {
		return err
	}

	_, err = f.Write(out.Bytes())
	if err != nil {
		return err
	}

	return nil
}
