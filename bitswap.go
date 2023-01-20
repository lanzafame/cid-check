package main

import (
	"context"
	"fmt"
	"time"

	bsmsg "github.com/ipfs/go-bitswap/message"
	bsmsgpb "github.com/ipfs/go-bitswap/message/pb"
	bsnet "github.com/ipfs/go-bitswap/network"
	"github.com/ipfs/go-cid"
	nrouting "github.com/ipfs/go-ipfs-routing/none"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/routing"
)

var nilRouter routing.Routing

func init() {
	nr, err := nrouting.ConstructNilRouting(context.TODO(), nil, nil, nil)
	if err != nil {
		panic(err)
	}
	nilRouter = nr
}

type BS struct {
	bsnet.BitSwapNetwork
	rcv *bsReceiver
}

func (bs BS) checkBitswapCIDs(ctx context.Context, cs []cid.Cid, ai peer.AddrInfo) ([]*BsCheckOutput, int) {
	target := ai.ID

	msg := bsmsg.New(false)
	for _, c := range cs {
		msg.AddEntry(c, 0, bsmsgpb.Message_Wantlist_Have, true)
	}

	start := time.Now()

	output := []*BsCheckOutput{}
	if err := bs.SendMessage(ctx, target, msg); err != nil {
		output = append(output, &BsCheckOutput{
			Duration:  time.Since(start),
			Found:     false,
			Responded: false,
			Error:     err.Error(),
		})
		return output, len(cs)
	}

	// in case for some reason we're sent a bunch of messages (e.g. wants) from a peer without them responding to our query
	// FIXME: Why would this be the case?
	// sctx, cancel := context.WithTimeout(ctx, time.Second*10)
	// defer cancel()
	// loop:
	for {
		// var res msgOrErr
		// select {
		// case res = <-bs.rcv.result:
		// case <-sctx.Done():
		// 	break loop
		// }
		res := <-bs.rcv.result

		if res.err != nil {
			output = append(output, &BsCheckOutput{
				Duration:  time.Since(start),
				Found:     false,
				Responded: true,
				Error:     res.err.Error(),
			})
			return output, len(cs)
		}

		if res.msg == nil {
			panic("should not be reachable")
		}

		for _, msgC := range res.msg.Blocks() {
			for i, c := range cs {
				if msgC.Cid().Equals(c) {
					output = append(output, &BsCheckOutput{
						Cid:       c,
						Duration:  time.Since(start),
						Found:     true,
						Responded: true,
						Error:     "",
					})
					cs = append(cs[:i], cs[i+1:]...)
				}
			}
		}

		for _, msgC := range res.msg.Haves() {
			for i, c := range cs {
				if msgC.Equals(c) {
					output = append(output, &BsCheckOutput{
						Cid:       c,
						Duration:  time.Since(start),
						Found:     true,
						Responded: true,
						Error:     "",
					})
					cs = append(cs[:i], cs[i+1:]...)
				}
			}
		}

		for _, msgC := range res.msg.DontHaves() {
			for i, c := range cs {
				if msgC.Equals(c) {
					output = append(output, &BsCheckOutput{
						Cid:       c,
						Duration:  time.Since(start),
						Found:     false,
						Responded: true,
						Error:     "",
					})
					cs = append(cs[:i], cs[i+1:]...)
				}
			}
		}
		if len(cs) == 0 {
			return output, 0
		}
	}
	// return output, len(cs)
}

func (bs BS) checkBitswapCID(ctx context.Context, c cid.Cid, msg bsmsg.BitSwapMessage, ai peer.AddrInfo) *BsCheckOutput {
	target := ai.ID

	rcv := &bsReceiver{
		target: target,
		result: make(chan msgOrErr),
	}

	start := time.Now()

	if err := bs.SendMessage(ctx, target, msg); err != nil {
		return &BsCheckOutput{
			Duration:  time.Since(start),
			Found:     false,
			Responded: false,
			Error:     err.Error(),
		}
	}

	// in case for some reason we're sent a bunch of messages (e.g. wants) from a peer without them responding to our query
	// FIXME: Why would this be the case?
	sctx, cancel := context.WithTimeout(ctx, time.Second*10)
	defer cancel()
loop:
	for {
		var res msgOrErr
		select {
		case res = <-rcv.result:
		case <-sctx.Done():
			break loop
		}

		if res.err != nil {
			return &BsCheckOutput{
				Duration:  time.Since(start),
				Found:     false,
				Responded: true,
				Error:     res.err.Error(),
			}
		}

		if res.msg == nil {
			panic("should not be reachable")
		}

		// for _, msgC := range res.msg.Blocks() {
		// 	if msgC.Cid().Equals(c) {
		// 		return &BsCheckOutput{
		// 			Duration:  time.Since(start),
		// 			Found:     true,
		// 			Responded: true,
		// 			Error:     "",
		// 		}
		// 	}
		// }

		for _, msgC := range res.msg.Haves() {
			if msgC.Equals(c) {
				return &BsCheckOutput{
					Duration:  time.Since(start),
					Found:     true,
					Responded: true,
					Error:     "",
				}
			}
		}

		for _, msgC := range res.msg.DontHaves() {
			if msgC.Equals(c) {
				return &BsCheckOutput{
					Duration:  time.Since(start),
					Found:     false,
					Responded: true,
					Error:     "",
				}
			}
		}
	}

	return &BsCheckOutput{
		Duration:  time.Since(start),
		Found:     false,
		Responded: false,
		Error:     "",
	}
}

type bsReceiver struct {
	target peer.ID
	result chan msgOrErr
}

type msgOrErr struct {
	msg bsmsg.BitSwapMessage
	err error
}

func (r *bsReceiver) ReceiveMessage(ctx context.Context, sender peer.ID, incoming bsmsg.BitSwapMessage) {
	if r.target != sender {
		select {
		case <-ctx.Done():
		case r.result <- msgOrErr{err: fmt.Errorf("expected peerID %v, got %v", r.target, sender)}:
		}
		return
	}

	select {
	case <-ctx.Done():
	case r.result <- msgOrErr{msg: incoming}:
	}
}

func (r *bsReceiver) ReceiveError(err error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	select {
	case <-ctx.Done():
	case r.result <- msgOrErr{err: err}:
	}
}

func (r *bsReceiver) PeerConnected(id peer.ID) {}

func (r *bsReceiver) PeerDisconnected(id peer.ID) {}

var _ bsnet.Receiver = (*bsReceiver)(nil)
