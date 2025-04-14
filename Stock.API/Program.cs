using MassTransit;
using Shared;
using Stock.API.Consumers;
using MongoDB.Driver;
using Stock.API.Services;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddMassTransit(configurator =>
{
    configurator.AddConsumer<OrderCreatedEventConsumer>();
    configurator.AddConsumer<PaymentFailedEventConsumer>();
    configurator.UsingRabbitMq((context, _configure) =>
    {
        _configure.Host(builder.Configuration["RabbitMQ"]);
        _configure.ReceiveEndpoint(RabbitMQSettings.Stock_OrderCreatedEventQueue, e => e.ConfigureConsumer<OrderCreatedEventConsumer>(context));
        _configure.ReceiveEndpoint(RabbitMQSettings.Stock_PaymentFailedEventQueue, e => e.ConfigureConsumer<PaymentFailedEventConsumer>(context));
    });
});

builder.Services.AddSingleton<MongoDBService>();

var app = builder.Build();

using IServiceScope scope = app.Services.CreateScope();
MongoDBService mongoDBService = scope.ServiceProvider.GetRequiredService<MongoDBService>();

var stockCollection = mongoDBService.GetCollection<Stock.API.Models.Stock>();
if ((await stockCollection.FindAsync(session => true)).Any())
{
    await stockCollection.InsertOneAsync(new() { Count = 100, ProductId = Guid.NewGuid().ToString() });
    await stockCollection.InsertOneAsync(new() { Count = 200, ProductId = Guid.NewGuid().ToString() });
    await stockCollection.InsertOneAsync(new() { Count = 50, ProductId = Guid.NewGuid().ToString() });
    await stockCollection.InsertOneAsync(new() { Count = 30, ProductId = Guid.NewGuid().ToString() });
    await stockCollection.InsertOneAsync(new() { Count = 5, ProductId = Guid.NewGuid().ToString() });
}

app.Run();
