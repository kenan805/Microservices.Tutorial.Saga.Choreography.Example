using MassTransit;
using Shared.Events;

namespace Payment.API.Consumers;

public class StockReservedEventConsumer(IPublishEndpoint publishEndpoint) : IConsumer<StockReservedEvent>
{
    public async Task Consume(ConsumeContext<StockReservedEvent> context)
    {
        if (true)
        {
            // Payment success...
            await publishEndpoint.Publish(new PaymentCompletedEvent
            {
                OrderId = context.Message.OrderId
            });
            await Console.Out.WriteLineAsync("Payment success...");
        }
        else
        {
            // Payment failed...
            PaymentFailedEvent paymentFailedEvent = new()
            {
                OrderId = context.Message.OrderId,
                Message = "Payment failed...",
                OrderItems = context.Message.OrderItems
            };
            await publishEndpoint.Publish(paymentFailedEvent);
            await Console.Out.WriteLineAsync("Payment failed...");
        }
    }
}
