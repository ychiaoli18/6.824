package paxos

//
// Paxos library, to be included in an application.
// Multiple applications will run, each including
// a Paxos peer.
//
// Manages a sequence of agreed-on values.
// The set of peers is fixed.
// Copes with network failures (partition, msg loss, &c).
// Does not store anything persistently, so cannot handle crash+restart.
//
// The application interface:
//
// px = paxos.Make(peers []string, me string)
// px.Start(seq int, v interface{}) -- start agreement on new instance
// px.Status(seq int) (decided bool, v interface{}) -- get info about an instance
// px.Done(seq int) -- ok to forget all instances <= seq
// px.Max() int -- highest instance seq known, or -1
// px.Min() int -- instances before this seq have been forgotten
//

/* 
  Note: https://pdos.csail.mit.edu/6.824/labs/lab-3.html

  Paxos Pseudo Code
    proposer(v):
      while not decided:
        choose n, unique and higher than any n seen so far
        send prepare(n) to all servers including self
        if prepare_ok(n, n_a, v_a) from majority:
          v' = v_a with highest n_a; choose own v otherwise
          send accept(n, v') to all
          if accept_ok(n) from majority:
            send decided(v') to all
    
    acceptor's state:
      n_p (highest prepare seen)
      n_a, v_a (highest accept seen)
    
    acceptor's prepare(n) handler:
      if n > n_p
        n_p = n
        reply prepare_ok(n, n_a, v_a)
      else
        reply prepare_reject
    
    acceptor's accept(n, v) handler:
      if n >= n_p
        n_p = n
        n_a = n
        v_a = v
        reply accept_ok(n)
      else
        reply accept_reject
*/

import "net"
import "net/rpc"
import "log"
import "os"
import "syscall"
import "sync"
import "fmt"
import "time"
import "strconv"
import "math/rand"

const (
  EMPTY = "0"
  OK = "OK"
  REJECT = "REJECT"
)


type Paxos struct {
  mu sync.Mutex
  l net.Listener
  dead bool
  unreliable bool
  rpcCount int
  peers []string
  me int // index into peers[]


  // Your data here.

  instances map[int]*PaxosInstance
  dones []int
}

type Proposal struct {
  ballot string
  val interface {}
}

type PaxosInstance struct {
  pid int
  val interface {}
  decided bool
  prepared_ballot string
  accepted_proposal Proposal
}

type RPCArg struct {
  Pid int
  Ballot string
  Val interface {}
  Me int
  Done int
}

type RPCReply struct {
  Result string // enum(reject, ok)
  Ballot string
  Val interface {}
}


func (px *Paxos) generateBallot() string {
  begin := time.Date(2014, time.May, 6, 23, 0, 0, 0, time.UTC)
  duration := time.Now().Sub(begin)
  return strconv.FormatInt(duration.Nanoseconds(), 10) + "-" + strconv.Itoa(px.me)
}

func (px *Paxos) sendPrepare (pid int, val interface {}) (bool, Proposal) {

  ballot := px.generateBallot()
  proposal := Proposal{ EMPTY, val }

  nOk := 0
  for i, acceptor := range px.peers {

    arg := RPCArg{ Pid: pid, Ballot: ballot, Me: px.me }
    reply := RPCReply{ Result: REJECT }
    if (i == px.me) {
      px.ProcessPrepare(&arg, &reply)
    } else {
      call(acceptor, "Paxos.ProcessPrepare", &arg, &reply)
      if reply.Result == OK {
        if reply.Ballot > proposal.ballot {
          proposal.ballot = reply.Ballot
          proposal.val = reply.Val
        }
        nOk++
      }
    }
  }
  return px.IsMajority(nOk), proposal
}

func (px *Paxos) sendAccept (pid int, proposal Proposal) bool {

  nOk := 0
  for i, acceptor := range px.peers {

    arg := RPCArg{ Pid: pid, Ballot: proposal.ballot, Val: proposal.val, Me: px.me }
    reply := RPCReply{ Result: REJECT }

    if i == px.me {
      px.ProcessAccept(&arg, &reply)
    } else {
      call(acceptor, "Paxos.ProcessAccept", &arg, &reply)
      if reply.Result == OK {
        nOk++
      }
    }
  }
  return px.IsMajority(nOk)
}

func (px *Paxos) sendDecision(pid int, proposal Proposal) {

  arg := RPCArg{ Pid: pid, Ballot: proposal.ballot, Val: proposal.val, Done: px.dones[px.me], Me: px.me }
  reply := RPCReply{}
  px.makeDecision(pid, proposal)
  for i, peer := range px.peers {
    if i != px.me {
      call(peer, "Paxos.ProcessDecision", &arg, &reply)
    }
  }

}

func (px *Paxos) makeDecision(pid int, proposal Proposal) {
  if _, exist := px.instances[pid]; !exist {
    px.instances[pid] = &PaxosInstance{
      pid: pid,
      val: nil,
      decided: true,
      prepared_ballot: EMPTY,
      accepted_proposal: proposal}
  } else {
    px.instances[pid].decided = true
    px.instances[pid].accepted_proposal = proposal
  }
}

func (px *Paxos) ProcessDecision(arg *RPCArg, reply *RPCReply) error {
  px.mu.Lock()
  defer px.mu.Unlock()
  px.makeDecision(arg.Pid, Proposal{ ballot: arg.Ballot, val: arg.Val })
  return nil
}

func (px *Paxos) ProcessPrepare (arg *RPCArg, reply *RPCReply) error {
// acceptor's state:
//   n_p (highest prepare seen)
//   n_a, v_a (highest accept seen)
// 
// acceptor's prepare(n) handler:
//   if n > n_p
//     n_p = n
//     reply prepare_ok(n, n_a, v_a)
//   else
//     reply prepare_reject

  px.mu.Lock()
  defer px.mu.Unlock()

  instance, exist := px.instances[arg.Pid]

  if !exist {
    reply.Result = OK
    px.instances[arg.Pid] = &PaxosInstance{
      pid: arg.Pid,
      val: nil,
      decided: false,
      prepared_ballot: arg.Ballot,
      accepted_proposal: Proposal{ballot: arg.Ballot}}

  } else if arg.Ballot > instance.prepared_ballot {
    reply.Result = OK
    reply.Ballot = instance.accepted_proposal.ballot
    reply.Val = instance.accepted_proposal.val
    instance.prepared_ballot = arg.Ballot
  }
  return nil
}

func (px *Paxos) ProcessAccept (arg *RPCArg, reply *RPCReply) error {
// acceptor's accept(n, v) handler:
//   if n >= n_p
//     n_p = n
//     n_a = n
//     v_a = v
//     reply accept_ok(n)
//   else
//     reply accept_reject

  px.mu.Lock()
  defer px.mu.Unlock()

  instance, exist := px.instances[arg.Pid]

  if !exist {
    reply.Result = OK
    px.instances[arg.Pid] = &PaxosInstance{
      pid: arg.Pid,
      val: arg.Val,
      decided: false,
      prepared_ballot: arg.Ballot,
      accepted_proposal: Proposal{ballot: arg.Ballot}}

  } else if arg.Ballot >= instance.accepted_proposal.ballot {
    reply.Result = OK
    instance.prepared_ballot = arg.Ballot
    instance.accepted_proposal = Proposal{ ballot: arg.Ballot, val: arg.Val }
  }

  return nil
}

func (px *Paxos) doPropose (pid int, val interface {}) {
// proposer(v):
//   while not decided:
//     choose n, unique and higher than any n seen so far
//     send prepare(n) to all servers including self
//     if prepare_ok(n, n_a, v_a) from majority:
//       v' = v_a with highest n_a; choose own v otherwise
//       send accept(n, v') to all
//       if accept_ok(n) from majority:
//         send decided(v') to all
  i := 1
  for {
    // proposal contains (n, v')
    prepare_ok, proposal := px.sendPrepare(pid, val)

    accept_ok := false
    if prepare_ok {
      accept_ok = px.sendAccept(pid, proposal)
    }
    if accept_ok {
      px.sendDecision(pid, proposal)
      break
    }
    i++
  }
}

func (px *Paxos) IsMajority (nOk int) bool {
  return nOk > len(px.peers) / 2
}

//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the replys contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it does not get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please do not change this function.
//
func call(srv string, name string, args interface{}, reply interface{}) bool {
  c, err := rpc.Dial("unix", srv)
  if err != nil {
    err1 := err.(*net.OpError)
    if err1.Err != syscall.ENOENT && err1.Err != syscall.ECONNREFUSED {
      fmt.Printf("paxos Dial() failed: %v\n", err1)
    }
    return false
  }
  defer c.Close()

  err = c.Call(name, args, reply)
  if err == nil {
    return true
  }

  fmt.Println(err)
  return false
}


//
// the application wants paxos to start agreement on
// instance seq, with proposed value v.
// Start() returns right away; the application will
// call Status() to find out if/when agreement
// is reached.
//
func (px *Paxos) Start(seq int, v interface{}) {
  // Your code here.

  go func () {
    if seq < px.Min() {
      fmt.Println("returned...")
      return
    }
    px.doPropose(seq, v)
  } ()

}

//
// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
//
func (px *Paxos) Done(seq int) {
  // Your code here.
}

//
// the application wants to know the
// highest instance sequence known to
// this peer.
//
func (px *Paxos) Max() int {
  // Your code here.
  max := 0
  for k, _ := range px.instances {
    if k > max {
      max = k
    }
  }
  return max
}

//
// Min() should return one more than the minimum among z_i,
// where z_i is the highest number ever passed
// to Done() on peer i. A peers z_i is -1 if it has
// never called Done().
//
// Paxos is required to have forgotten all information
// about any instances it knows that are < Min().
// The point is to free up memory in long-running
// Paxos-based servers.
//
// Paxos peers need to exchange their highest Done()
// arguments in order to implement Min(). These
// exchanges can be piggybacked on ordinary Paxos
// agreement protocol messages, so it is OK if one
// peers Min does not reflect another Peers Done()
// until after the next instance is agreed to.
//
// The fact that Min() is defined as a minimum over
// *all* Paxos peers means that Min() cannot increase until
// all peers have been heard from. So if a peer is dead
// or unreachable, other peers Min()s will not increase
// even if all reachable peers call Done. The reason for
// this is that when the unreachable peer comes back to
// life, it will need to catch up on instances that it
// missed -- the other peers therefor cannot forget these
// instances.
// 
func (px *Paxos) Min() int {
  // You code here.
  return -1
}

//
// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so what the agreed value is. Status()
// should just inspect the local peer state;
// it should not contact other Paxos peers.
//
func (px *Paxos) Status(seq int) (bool, interface{}) {
  // Your code here.
  min := px.Min()

  if seq < min {
    return false, nil
  }

  instance, exist := px.instances[seq]
  if exist && instance.decided {
    return true, instance.accepted_proposal.val
  }
  return false, nil
}


//
// tell the peer to shut itself down.
// for testing.
// please do not change this function.
//
func (px *Paxos) Kill() {
  px.dead = true
  if px.l != nil {
    px.l.Close()
  }
}

//
// the application wants to create a paxos peer.
// the ports of all the paxos peers (including this one)
// are in peers[]. this servers port is peers[me].
//
func Make(peers []string, me int, rpcs *rpc.Server) *Paxos {
  px := &Paxos{}
  px.peers = peers
  px.me = me


  // Your initialization code here.
  px.instances = map[int]*PaxosInstance{}
  px.dones = make([]int, len(peers))
  for i:= range peers {
    px.dones[i] = -1
  }

  if rpcs != nil {
    // caller will create socket &c
    rpcs.Register(px)
  } else {
    rpcs = rpc.NewServer()
    rpcs.Register(px)

    // prepare to receive connections from clients.
    // change "unix" to "tcp" to use over a network.
    os.Remove(peers[me]) // only needed for "unix"
    l, e := net.Listen("unix", peers[me]);
    if e != nil {
      log.Fatal("listen error: ", e);
    }
    px.l = l

    // please do not change any of the following code,
    // or do anything to subvert it.

    // create a thread to accept RPC connections
    go func() {
      for px.dead == false {
        conn, err := px.l.Accept()
        if err == nil && px.dead == false {
          if px.unreliable && (rand.Int63() % 1000) < 100 {
            // discard the request.
            conn.Close()
          } else if px.unreliable && (rand.Int63() % 1000) < 200 {
            // process the request but force discard of reply.
            c1 := conn.(*net.UnixConn)
            f, _ := c1.File()
            err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
            if err != nil {
              fmt.Printf("shutdown: %v\n", err)
            }
            px.rpcCount++
            go rpcs.ServeConn(conn)
          } else {
            px.rpcCount++
            go rpcs.ServeConn(conn)
          }
        } else if err == nil {
          conn.Close()
        }
        if err != nil && px.dead == false {
          fmt.Printf("Paxos(%v) accept: %v\n", me, err.Error())
        }
      }
    }()
  }


  return px
}
