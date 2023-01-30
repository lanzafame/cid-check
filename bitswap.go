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
	"github.com/libp2p/go-libp2p/core/host"
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

func haveCIDs(ctx context.Context, testHost host.Host, ai *peer.AddrInfo, target peer.ID, cs []cid.Cid, results chan msgOrErr) error {
	start := time.Now()
	rcv := &bsReceiver{
		target: target,
		result: make(chan msgOrErr),
	}

	bs := BS{bsnet.NewFromIpfsHost(testHost, nilRouter), rcv}
	bs.Start(bs.rcv)
	defer bs.Stop()

	msg := bsmsg.New(false)
	for _, c := range cs {
		msg.AddEntry(c, 0, bsmsgpb.Message_Wantlist_Have, true)
	}

	if err := bs.SendMessage(ctx, bs.rcv.target, msg); err != nil {
		return nil
	}

	for {
		select {
		case <-ctx.Done():
			return nil
		case res := <-bs.rcv.result:
			res.start = start
			results <- res
			return nil
		}
	}
}

type bsReceiver struct {
	target peer.ID
	result chan msgOrErr
}

type msgOrErr struct {
	msg   bsmsg.BitSwapMessage
	err   error
	start time.Time
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
