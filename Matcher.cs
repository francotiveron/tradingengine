using Akka.Actor;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace TradingEngine
{
    public class Matcher : UntypedActor
    {
        private readonly string _stockId;
        Queue<object> messages = new Queue<object>();
        List<Order> Bids { get; } = new List<Order>();
        List<Order> Asks { get; } = new List<Order>();
        IEnumerable<Order> All => Bids.Concat(Asks);
        List<Trade> Trades { get; } = new List<Trade>();
        bool run = true;

        decimal? bid;
        bool bidChanged = false;
        public decimal? Bid { 
            get => bid;
            private set
            {
                bidChanged = value != bid;
                bid = value;
            }
        }

        decimal? ask;
        bool askChanged = false;
        public decimal? Ask
        {
            get => ask;
            private set
            {
                askChanged = value != ask;
                ask = value;
            }
        }

        public Matcher(string stockId)
        {
            _stockId = stockId;
        }

        void Raise(object evt) => Context.System.EventStream.Publish(evt);
        void RaisePriceChanged()
        {
            if (askChanged || bidChanged)
            {
                Raise(new PriceChanged { StockId = _stockId, Bid = Bid, Ask = Ask });
                askChanged = bidChanged = false;
            }
        }
        void RaiseTradeSettled(Trade trade)
        {
            (Order bidOrder, Order askOrder) = trade.Order1.IsBid ? (trade.Order1, trade.Order2) : (trade.Order2, trade.Order1);
            Raise (new TradeSettled
                {
                    StockId = _stockId,
                    BidOrderId = bidOrder.OrderId,
                    AskOrderId = askOrder.OrderId,
                    Price = trade.Price,
                    Units = trade.Units
                });
        }

        void RaiseOrderPlaced(Order order) => Raise(new OrderPlaced { Order = order });

        void UpdateBid() => Bid = Bids.Count > 0 ? (decimal?)Bids.Max(ord => ord.Price) : null;
        void UpdateAsk() => Ask = Asks.Count > 0 ? (decimal?)Asks.Min(ord => ord.Price) : null;

        void Add(Order order)
        {
            if (order.IsBid)
            {
                Bids.Add(order);
                UpdateBid();
            }
            else
            {
                Asks.Add(order);
                UpdateAsk();
            }
            messages.Enqueue(order);
            RaiseOrderPlaced(order);
            RaisePriceChanged();
        }
        void Remove(Order order)
        {
            if (order.IsBid)
            {
                Bids.Remove(order);
                UpdateBid();
            }
            else
            {
                Asks.Remove(order);
                UpdateAsk();
            }
        }
        bool IsInvalid(Order order) => order.Price <= 0m || order.Units <= 0 || All.Any(ord => ord.OrderId == order.OrderId);

        protected override void OnReceive(object message) //=> Unhandled(message);
        {
            switch (message)
            {
                case Bid bidMsg:
                    if (IsInvalid(bidMsg.Order))
                    {
                        Sender.Tell(new BidResult { Success = false, Reason = "Invalid Order" });
                        break;
                    }
                    Sender.Tell(new BidResult { Success = true, Reason = "Valid Order" });

                    Add(bidMsg.Order);
                    Self.Tell("Next");
                    break;

                case Ask askMsg:
                    if (IsInvalid(askMsg.Order))
                    {
                        Sender.Tell(new AskResult { Success = false, Reason = "Invalid Order" });
                        break;
                    }
                    Sender.Tell(new AskResult { Success = true, Reason = "Valid Order" });

                    Add(askMsg.Order);
                    Self.Tell("Next");
                    break;

                case "Next":
                    if (run && messages.TryDequeue(out var msg))
                    {
                        Self.Tell(msg);
                        Self.Tell("Next");
                    }
                    break;

                case Order order:
                    Process(order);
                    break;

                case GetPrice _:
                    bool success = false;
                    string reason = "Price Unavailable";

                    if (Asks.Count > 0 && Bids.Count > 0)
                    {
                        success = true;
                        reason = "Price Available";
                    }

                    Sender.Tell(new GetPriceResult { Ask = Ask, Bid = Bid, Success = success, Reason = reason });
                    break;

                case GetTrades _:
                    var res = new GetTradesResult();
                    res.Success = false;
                    res.Reason = "No order has been executed";
                    res.Orders = new List<Order>();
                    if (Trades.Count > 0)
                    {
                        foreach(var trade in Trades)
                        {
                            res.Orders.Add(trade.Order1);
                            res.Orders.Add(trade.Order2);
                        }
                        res.Success = true;
                        res.Reason = $"{res.Orders.Count} Orders Filled";
                    }
                    Sender.Tell(res);
                    break;

                case Start _:
                    run = true;
                    Self.Tell("Next");
                    break;

                case Halt _:
                    run = false;
                    break;

                case "AllDone":
                    Sender.Tell(messages.Count == 0);
                    break;
            }

            void Process(Order order)
            {
                var (orders, oppositeOrders, selector) = order.IsBid ? 
                    (Bids, Asks, new Func<Order, bool>((Order o) => o.Price <= order.Price)) : 
                    (Asks, Bids, new Func<Order, bool>((Order o) => o.Price >= order.Price));

                var fillable = oppositeOrders.Where(selector).ToList();
                foreach (var opposite in fillable)
                {
                    var units = Math.Min(order.Units, opposite.Units);
                    var trade = new Trade(order, opposite, opposite.Price, units);
                    RaiseTradeSettled(trade);
                    Trades.Add(trade);
                    opposite.Units -= units;
                    if (opposite.Units <= 0) Remove(opposite);
                    order.Units -= units;
                    if (order.Units <= 0) Remove(order);
                }
                RaisePriceChanged();
            }
        }
    }
    class Trade
    {
        public Order Order1 { get; }
        public Order Order2 { get; }
        public decimal Price { get; }
        public int Units { get; }

        public Trade(Order order1, Order order2, decimal price, int units)
        {
            Order1 = order1;
            Order2 = order2;
            Price = price;
            Units = units;
        }
    }

}