using System;
using System.Collections.Generic;
using Akka.Actor;
using Akka.Actor.Dsl;

namespace TradingEngine
{
    class Program
    {
        static void Main(string[] args)
        {
            using var system = ActorSystem.Create("trade");

            var matcher = system.ActorOf(Props.Create(() => new Matcher("MSFT")));

            // Place a few orders
            matcher.Tell(Ask.New("1", "MSFT", units: 50, price: 99.00m), ActorRefs.Nobody);
            matcher.Tell(Bid.New("2", "MSFT", units: 100, price: 100.00m), ActorRefs.Nobody);
            matcher.Tell(Ask.New("3", "MSFT", units: 50, price: 100.00m), ActorRefs.Nobody);

            // Get the current price
            matcher.Ask<GetPriceResult>(Ask.New("3", "MSFT", units: 50, price: 100.00m))
                .ContinueWith(r => Console.WriteLine($"MSFT bid:{r.Result.Bid:c2} ask:{r.Result.Ask:c2}"));

            // Listen for trade settlement and price changes
            var logger = system.ActorOf(dsl =>
            {
                dsl.Receive<TradeSettled>((evt, ctx) => 
                    Console.WriteLine($"! Settled bid {evt.BidOrderId} with ask {evt.AskOrderId}: {evt.Units} @ {evt.Price:c2}"));
                dsl.Receive<PriceChanged>((evt, ctx) => 
                    Console.WriteLine($"! {evt.StockId} now at bid: {evt.Bid:c2} ask: {evt.Ask:c2}"));
                dsl.Receive<OrderPlaced>((evt, ctx) => 
                    Console.WriteLine($"! Order placed: {evt.Order}"));
            });

            // Subscribe to trade settlement, price change, and order placed events
            system.EventStream.Subscribe(logger, typeof(TradeSettled));
            system.EventStream.Subscribe(logger, typeof(PriceChanged));
            system.EventStream.Subscribe(logger, typeof(OrderPlaced));

            // Get the full list of successful trades
            var trades = matcher.Ask<GetTradesResult>(new GetTrades())
                .ContinueWith(r => Console.WriteLine(string.Join(Environment.NewLine, r.Result.Orders)));

            Console.WriteLine("Press enter to stop trading engine");
            Console.ReadLine();
        }
    }

    #region Protocol

    /// <summary>
    /// Place a bid order (buy)
    /// </summary>
    public class Bid
    {
        private Bid() { }
        public Order Order { get; private set; }
        public static Bid New(string id, string code, int units, decimal price) =>
            new Bid { Order = new Order { IsBid = true, OrderId = id, StockId = code, Units = units, Price = price } };
    }
    public class BidResult
    {
        public bool Success { get; set; }
        public string Reason { get; set; }
    }

    /// <summary>
    /// Place an ask order (sell)
    /// </summary>
    public class Ask
    {
        private Ask() { }
        public Order Order { get; set; }
        public static Ask New(string id, string code, int units, decimal price) =>
            new Ask { Order = new Order { IsBid = false, OrderId = id, StockId = code, Units = units, Price = price } };
    }
    public class AskResult
    {
        public bool Success { get; set; }
        public string Reason { get; set; }
    }

    /// <summary>
    /// Get the current best bid and ask price
    /// </summary>
    public class GetPrice
    {
        public string StockId { get; set; }
        public decimal Price { get; set; }
        public int Units { get; set; }
    }
    public class GetPriceResult
    {
        public bool Success { get; set; }
        public string Reason { get; set; }
        public decimal? Ask { get; set; }
        public decimal? Bid { get; set; }
    }

    /// <summary>
    /// Get settled trades
    /// </summary>
    public class GetTrades
    {
        public string StockId { get; set; }
    }
    public class GetTradesResult
    {
        public bool Success { get; set; }
        public string Reason { get; set; }
        public IList<Order> Orders { get; set; }
    }

    /// <summary>
    /// Start the trading engine
    /// </summary>
    public class Start
    {
        public string StockId { get; set; }
    }
    public class StartResult
    {
        public bool Success { get; set; }
        public string Reason { get; set; }
    }

    /// <summary>
    /// Halt the trading engine
    /// </summary>
    public class Halt
    {
        public string StockId { get; set; }
    }
    public class HaltResult
    {
        public bool Success { get; set; }
        public string Reason { get; set; }
    }

    /// <summary>
    /// Event that fires when shares changed hands
    /// </summary>
    public class TradeSettled
    {
        public string BidOrderId { get; set; }
        public string AskOrderId { get; set; }
        public string StockId { get; set; }
        public decimal Price { get; set; }
        public int Units { get; set; }
    }

    /// <summary>
    /// Event fires when share price changes (i.e. best bid price or best ask price changes)
    /// </summary>
    public class PriceChanged
    {
        public string StockId { get; set; }
        public decimal? Bid { get; set; }
        public decimal? Ask { get; set; }
    }

    /// <summary>
    /// Event fires when a bid or ask order has been accepted (but not necessarily processed)
    /// </summary>
    public class OrderPlaced
    {
        public Order Order { get; set; }
    }

    /// <summary>
    /// Data type that specifies a fully specified order
    /// </summary>
    public class Order
    {
        public string OrderId { get; set; }
        public bool IsBid { get; set; }
        public string StockId { get; set; }
        public decimal Price { get; set; }
        public int Units { get; set; }

        public override string ToString() =>
          $"{(IsBid ? "Bid" : "Ask")} order {OrderId}: {StockId} {Units} {Price:c2}";
    }

    #endregion
}
