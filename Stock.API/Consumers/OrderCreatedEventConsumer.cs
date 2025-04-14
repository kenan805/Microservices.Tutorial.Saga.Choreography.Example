using MassTransit;
using MongoDB.Driver;
using Shared;
using Shared.Events;
using Stock.API.Models;
using Stock.API.Services;

namespace Stock.API.Consumers;

public class OrderCreatedEventConsumer(MongoDBService mongoDBService, ISendEndpointProvider sendEndpointProvider, IPublishEndpoint publishEndpoint) : IConsumer<OrderCreatedEvent>
{
    public async Task Consume(ConsumeContext<OrderCreatedEvent> context)
    {
        List<bool> stockResult = new();
        IMongoCollection<Models.Stock> collection = mongoDBService.GetCollection<Models.Stock>();

        foreach (var orderItem in context.Message.OrderItems)
        {
            stockResult.Add(await (await collection.FindAsync(s => s.ProductId == orderItem.ProductId.ToString() && s.Count > orderItem.Count)).AnyAsync());
        }

        if (stockResult.TrueForAll(s => s.Equals(true)))
        {
            // Stock update
            foreach (var orderItem in context.Message.OrderItems)
            {
                //await collection.UpdateOneAsync(s => s.ProductId == orderItem.ProductId, Builders<Models.Stock>.Update.Inc(s => s.Count, -orderItem.Count));

                var stock = (await collection.FindAsync(s => s.ProductId == orderItem.ProductId.ToString())).FirstOrDefault();
                stock.Count -= orderItem.Count;

                await collection.FindOneAndReplaceAsync(s => s.ProductId == orderItem.ProductId.ToString(), stock);
            }

            // Send StockReservedEvent
            var sendEndPoint = await sendEndpointProvider.GetSendEndpoint(new Uri($"queue:{RabbitMQSettings.Payment_StockReservedEventQueue}"));
            StockReservedEvent stockReservedEvent = new()
            {
                BuyerId = context.Message.BuyerId,
                OrderId = context.Message.OrderId,
                TotalPrice = context.Message.TotalPrice,
                OrderItems = context.Message.OrderItems
            };

            await sendEndPoint.Send(stockReservedEvent);
        }
        else
        {
            // Send StockNotReservedEvent
            StockNotReservedEvent stockNotReservedEvent = new()
            {
                BuyerId = context.Message.BuyerId,
                OrderId = context.Message.OrderId,
                Message = "Stock miqdari kifayet etmir"
            };

            // Publish StockNotReservedEvent
            await publishEndpoint.Publish(stockNotReservedEvent);
        }

    }
}
