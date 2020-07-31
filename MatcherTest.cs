using Akka.Actor;
using Akka.TestKit.Xunit2;
using Xunit;

namespace TradingEngine
{
    public class MatcherTest : TestKit
    {
        private readonly IActorRef _matcher;

        public MatcherTest()
        {
            _matcher = Sys.ActorOf(Props.Create(() => new Matcher("MSFT")));
        }

        [Fact]
        public void Order_should_place()
        {
            _matcher.Tell(Ask.New("1", "MSFT", units: 50, price: 99.00m));
            ExpectMsg<AskResult>(r => Assert.True(r.Success, r.Reason));
        }
    }
}
