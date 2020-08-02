using Akka.Actor;
using Akka.TestKit.Xunit2;
using System.Collections.Generic;
using Akka.Actor.Dsl;
using System.Threading;
using Xunit;
using Akka.Util.Internal;
using System.Linq;

namespace TradingEngine
{
    public class MatcherTest : TestKit
    {
        private readonly IActorRef _matcher;
        int oId = 0;

        public MatcherTest()
        {
            _matcher = Sys.ActorOf(Props.Create(() => new Matcher("MSFT")));
        }

        string OID => (++oId).ToString();
        Order Buy(int units, decimal price)
        {
            var msg = Bid.New(OID, "MSFT", units: units, price: price);
            _matcher.Tell(msg);
            return msg.Order;
        }
        Order Sell(int units, decimal price)
        {
            var msg = Ask.New(OID, "MSFT", units: units, price: price);
            _matcher.Tell(msg);
            return msg.Order;
        }
        void _Buy(int units, decimal price)
        {
            var msg = Bid.New(OID, "MSFT", units: units, price: price);
            _matcher.Ask<BidResult>(msg);
        }
        void _Sell(int units, decimal price)
        {
            var msg = Ask.New(OID, "MSFT", units: units, price: price);
            _matcher.Ask<AskResult>(msg);
        }

        void AssertPrice(decimal bid, decimal ask)
        {
            var rsp = _matcher.Ask<GetPriceResult>(new GetPrice()).Result;
            Assert.Equal((bid, ask), (rsp.Bid, rsp.Ask));
        }
        void AssertTrades(IEnumerable<Order> orders)
        {
            var rsp = _matcher.Ask<GetTradesResult>(new GetTrades()).Result;
            var set1 = new HashSet<Order>(orders);
            var set2 = new HashSet<Order>(rsp.Orders);
            Assert.True(set1.SetEquals(set2));
        }
        [Fact]
        public void Sell_order_should_place()
        {
            Sell(50, 99m);
            ExpectMsg<AskResult>(r => Assert.True(r.Success, r.Reason));
        }

        [Fact]
        public void Buy_order_should_place()
        {
            Buy(50, 99m);
            ExpectMsg<BidResult>(r => Assert.True(r.Success, r.Reason));
        }

        [Fact]
        public void Invalid_order_should_fail()
        {
            Sell(0, 99m);
            ExpectMsg<AskResult>(r => Assert.False(r.Success, r.Reason));
        }
        [Fact]
        public void Current_Bid_Ask()
        {
            Buy(50, 99m);
            Sell(50, 100m);
            AssertPrice(99m, 100m);
        }
        [Fact]
        public void Current_Bids_Asks()
        {
            for (int i = 0; i < 100; i++)
            {
                Buy(1, i);
                Sell(1, 100 + i);
            }
            AssertPrice(99m, 100m);
        }
        [Fact]
        public void Simple_Trade()
        {
            var orders = new List<Order>();
            orders.Add(Buy(50, 100m));
            orders.Add(Sell(50, 100m));
            AssertTrades(orders);
        }
        [Fact]
        public void Two_Trades()
        {
            var orders = new List<Order>();
            orders.Add(Buy(50, 100m));
            orders.Add(Sell(10, 100m));
            orders.Add(Sell(10, 99m));
            AssertTrades(orders);
        }
        [Fact]
        public void Multiple_Orders()
        {
            var orders = new List<Order>();
            for (int i = 0; i < 100; i++)
            {
                var buy = i;
                var sell = 150 - i;
                var bid = Buy(1, buy);
                var ask = Sell(1, sell);
                if (buy > 50 && buy < 100) orders.Add(bid);
                if (sell > 50 && sell < 100) orders.Add(ask);
            }

            AssertTrades(orders);
        }
        [Fact]
        public void Halt_Test()
        {
            var a1 = Ask.New(OID, "MSFT", 10, 10m);
            var b1 = Bid.New(OID, "MSFT", 10, 10m);
            var a2 = Ask.New(OID, "MSFT", 10, 11m);
            var b2 = Bid.New(OID, "MSFT", 10, 11m);
            
            _matcher.Tell(a1);
            _matcher.Tell(b1);

            Order[] orders = { a1.Order, b1.Order };
            AssertTrades(orders);

            _matcher.Tell(new Halt());

            _matcher.Tell(a2);
            _matcher.Tell(b2);

            AssertTrades(orders);

            _matcher.Tell(new Start());

            _matcher.Tell(a2);
            _matcher.Tell(b2);

            AssertTrades(orders.AsEnumerable().Concat(new Order[] { a2.Order, b2.Order}));
        }
        [Fact]
        public void PriceChanged_Event()
        {
            var h = new AutoResetEvent(false);
            decimal? bid = null, ask = null;

            var logger = Sys.ActorOf(dsl =>
            {
                dsl.Receive<PriceChanged>((evt, ctx) => { bid = evt.Bid; ask = evt.Ask; h.Set(); });
            });
            Sys.EventStream.Subscribe(logger, typeof(PriceChanged));

            _Buy(1, 10m);
            Assert.True(h.WaitOne(1000));
            Assert.Equal(10m, bid);
            Assert.Null(ask);
            _Sell(1, 11m);
            Assert.True(h.WaitOne(1000));
            Assert.Equal(10m, bid);
            Assert.Equal(11m, ask);
        }
        [Fact]
        public void OrderPlaced_Event()
        {
            var h = new AutoResetEvent(false);
            Order o = null, oo = null;

            var logger = Sys.ActorOf(dsl =>
            {
                dsl.Receive<OrderPlaced>((evt, ctx) => { o = evt.Order;  h.Set(); });
            });
            Sys.EventStream.Subscribe(logger, typeof(OrderPlaced));
            oo = Buy(1, 10m);
            Assert.True(h.WaitOne(1000));
            Assert.Equal(o, oo);
            oo = Sell(1, 10m);
            Assert.True(h.WaitOne(1000));
            Assert.Equal(o, oo);
        }
        [Fact]
        public void TradeSettled_Event()
        {
            var h = new AutoResetEvent(false);
            int units = 0;
            decimal price = 0m;

            var logger = Sys.ActorOf(dsl =>
            {
                dsl.Receive<TradeSettled>((evt, ctx) => { units = evt.Units; price = evt.Price; h.Set(); });
            });
            Sys.EventStream.Subscribe(logger, typeof(TradeSettled));

            _Buy(76, price = 10m);
            _Sell(units = 45, 9m);
            Assert.True(h.WaitOne(1000));
            Assert.Equal((45, 10m), (units, price));
            _Sell(80, 9.5m);
            Assert.True(h.WaitOne(1000));
            Assert.Equal((31, 10m), (units, price));
            _Buy(100, 10.5m);
            Assert.True(h.WaitOne(1000));
            Assert.Equal((49, 9.5m), (units, price));
        }
    }
}
