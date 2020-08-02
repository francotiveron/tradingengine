using Akka.Actor;
using System;
using System.Collections.Generic;
using System.Linq;

namespace TradingEngine
{
    public class Matcher : UntypedActor
    {
        private readonly string _stockId;
        List<DynOrder> Bids { get; } = new List<DynOrder>();
        List<DynOrder> Asks { get; } = new List<DynOrder>();
        IEnumerable<DynOrder> All => Bids.Concat(Asks);
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

        void UpdateBid() => Bid = Bids.Count > 0 ? (decimal?)Bids.Max(ord => ord.Order.Price) : null;
        void UpdateAsk() => Ask = Asks.Count > 0 ? (decimal?)Asks.Min(ord => ord.Order.Price) : null;

        DynOrder Add(Order order)
        {
            var dynOrder = new DynOrder(order);
            if (order.IsBid)
            {
                Bids.Add(dynOrder);
                UpdateBid();
            }
            else
            {
                Asks.Add(dynOrder);
                UpdateAsk();
            }
            RaiseOrderPlaced(order);
            RaisePriceChanged();

            return dynOrder;
        }
        void Remove(DynOrder order)
        {
            if (order.Order.IsBid)
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
        bool IsInvalid(Order order) => order.Price <= 0m || order.Units <= 0 || All.Any(ord => ord.Order.OrderId == order.OrderId);

        protected override void OnReceive(object message) //=> Unhandled(message);
        {
            switch (message)
            {
                case Bid bidMsg:
                    BidResult bidResult;
                    if (run)
                    {
                        if (Process(bidMsg.Order)) bidResult = new BidResult { Success = true, Reason = "Valid Order" };
                        else bidResult = new BidResult { Success = false, Reason = "Invalid Order" };
                    }
                    else
                    {
                        bidResult = new BidResult { Success = false, Reason = "Engine Halted" };
                    }
                    Sender.Tell(bidResult);
                    break;

                case Ask askMsg:
                    AskResult askResult;
                    if (run)
                    {
                        if (Process(askMsg.Order)) askResult = new AskResult { Success = true, Reason = "Valid Order" };
                        else askResult = new AskResult { Success = false, Reason = "Invalid Order" };
                    }
                    else
                    {
                        askResult = new AskResult { Success = false, Reason = "Engine Halted" };
                    }
                    Sender.Tell(askResult);
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
                    break;

                case Halt _:
                    run = false;
                    break;
            }

            bool Process(Order ord)
            {
                if (IsInvalid(ord)) return false;
                
                var order = Add(ord);

                var (orders, oppositeOrders, selector) = order.Order.IsBid ? 
                    (Bids, Asks, new Func<DynOrder, bool>((DynOrder o) => o.Order.Price <= order.Order.Price)) : 
                    (Asks, Bids, new Func<DynOrder, bool>((DynOrder o) => o.Order.Price >= order.Order.Price));

                var fillable = oppositeOrders.Where(selector).ToList();
                foreach (var opposite in fillable)
                {
                    var units = Math.Min(order.Units, opposite.Units);
                    var trade = new Trade(order.Order, opposite.Order, opposite.Order.Price, units);
                    RaiseTradeSettled(trade);
                    Trades.Add(trade);
                    opposite.Units -= units;
                    if (opposite.Units <= 0) Remove(opposite);
                    order.Units -= units;
                    if (order.Units <= 0) Remove(order);
                    RaisePriceChanged();
                }

                return true;
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

    class DynOrder
    {
        public Order Order { get; }
        public int Units { get; set; }
        public DynOrder(Order order)
        {
            Order = order;
            Units = Order.Units;
        }
    }
}