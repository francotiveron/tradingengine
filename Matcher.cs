using Akka.Actor;

namespace TradingEngine
{
    public class Matcher : UntypedActor
    {
        private readonly string _stockId;

        public Matcher(string stockId)
        {
            _stockId = stockId;
        }

        protected override void OnReceive(object message) => Unhandled(message);
    }
}