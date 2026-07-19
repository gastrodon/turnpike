package turnpike

import (
	"context"
	"errors"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
)

func newTestRouter() *defaultRouter {
	router := NewDefaultRouter()
	router.RegisterRealm(URI("turnpike.test"), Realm{})
	return router.(*defaultRouter)
}

func connectedTestClients() (*Client, *Client) {
	router := newTestRouter()
	peer1 := router.getTestPeer()
	peer2 := router.getTestPeer()
	return newTestClient(peer1), newTestClient(peer2)
}

// testCtx returns a context bounded to 100ms so a regression that would
// otherwise hang the client forever fails the test quickly instead. cancel is
// registered on t.Cleanup so it always runs.
func testCtx(t *testing.T) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	t.Cleanup(cancel)
	return ctx
}

func newTestClient(p Peer) *Client {
	client := NewClient(p)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	_, err := client.JoinRealm(ctx, "turnpike.test", nil)
	So(err, ShouldBeNil)
	return client
}

func TestJoinRealm(t *testing.T) {
	Convey("Given a server accepting client connections", t, func() {
		peer := newTestRouter().getTestPeer()

		Convey("A client should be able to succesfully join a realm", func() {
			client := NewClient(peer)
			_, err := client.JoinRealm(context.Background(), "turnpike.test", nil)
			So(err, ShouldBeNil)
		})

		Convey("Joining with a cancelled context should return context.Canceled", func() {
			client := NewClient(peer)
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			_, err := client.JoinRealm(ctx, "turnpike.test", nil)
			So(err, ShouldEqual, context.Canceled)
		})
	})
}

func testAuthFunc(d map[string]interface{}, c map[string]interface{}) (string, map[string]interface{}, error) {
	return testCRSign(c), map[string]interface{}{}, nil
}

func TestJoinRealmWithAuth(t *testing.T) {
	Convey("Given a server accepting client connections", t, func() {
		router := newTestRouter()
		router.RegisterRealm(URI("turnpike.test.auth"), Realm{
			CRAuthenticators: map[string]CRAuthenticator{"testauth": &testCRAuthenticator{}},
		})

		peer := router.getTestPeer()

		Convey("A client should be able to successfully authenticate and join a realm", func() {
			client := NewClient(peer)
			client.Auth = map[string]AuthFunc{"testauth": testAuthFunc}
			details := map[string]interface{}{"username": "tester"}
			_, err := client.JoinRealm(context.Background(), "turnpike.test.auth", details)
			So(err, ShouldBeNil)
		})

		Convey("Authenticating with a cancelled context should return context.Canceled", func() {
			client := NewClient(peer)
			client.Auth = map[string]AuthFunc{"testauth": testAuthFunc}
			details := map[string]interface{}{"username": "tester"}
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			_, err := client.JoinRealm(ctx, "turnpike.test.auth", details)
			So(err, ShouldEqual, context.Canceled)
		})
	})
}

func TestRemoteCall(t *testing.T) {
	Convey("Given two clients connected to the same server", t, func() {
		callee, caller := connectedTestClients()

		Convey("The callee unregisters an invalid method", func() {
			err := callee.Unregister(testCtx(t), "invalidmethod")
			Convey("And expects an error", func() {
				So(err, ShouldNotBeNil)
			})
		})

		Convey("The callee registers a valid method", func() {
			handler := func(args []interface{}, kwargs map[string]interface{}) *CallResult {
				return &CallResult{Args: []interface{}{args[0].(int) * 2}}
			}
			methodName := "mymethod"
			err := callee.BasicRegister(testCtx(t), methodName, handler)

			Convey("And expects no error", func() {
				So(err, ShouldBeNil)

				Convey("The caller calls the callee's remote method", func() {
					callArgs := []interface{}{5100}
					result, err := caller.Call(testCtx(t), methodName, make(map[string]interface{}), callArgs, make(map[string]interface{}))

					Convey("And succeeds at multiplying the number by 2", func() {
						So(err, ShouldBeNil)
						So(result.Arguments[0], ShouldEqual, 10200)
					})
				})
			})

			Convey("And unregisters the method", func() {
				err := callee.Unregister(testCtx(t), methodName)
				Convey("And expects no error", func() {
					So(err, ShouldBeNil)
				})

				Convey("Calling the unregistered procedure", func() {
					callArgs := []interface{}{5100}
					result, err := caller.Call(testCtx(t), methodName, make(map[string]interface{}), callArgs, make(map[string]interface{}))

					Convey("Should result in an error", func() {
						So(err, ShouldNotBeNil)
						So(result, ShouldBeNil)
					})
				})
			})
		})
	})
}

func TestCallContextDeadline(t *testing.T) {
	Convey("Given two clients connected to the same server", t, func() {
		_, caller := connectedTestClients()

		Convey("Calling with an already-expired context returns context.DeadlineExceeded", func() {
			ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
			defer cancel()
			_, err := caller.Call(ctx, "mymethod", make(map[string]interface{}), nil, make(map[string]interface{}))
			So(errors.Is(err, context.DeadlineExceeded), ShouldBeTrue)
		})
	})
}
