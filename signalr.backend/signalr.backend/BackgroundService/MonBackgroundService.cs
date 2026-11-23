using Microsoft.AspNetCore.SignalR;
using signalr.backend.Data;
using signalr.backend.Hubs;

namespace signalr.backend.BackGroundService
{
    public class MonBackgroundService : BackgroundService
    {
        private IServiceScopeFactory _serviceScopeFactory;
        private IHubContext<ChatHub> _chatHubContext;

        public MonBackgroundService(
            IHubContext<ChatHub> chatHubContext,
            IServiceScopeFactory serviceScopeFactory)
        {
            _chatHubContext = chatHubContext;
            _serviceScopeFactory = serviceScopeFactory;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                using (IServiceScope scope = _serviceScopeFactory.CreateScope())
                {
                    ApplicationDbContext dbContext = scope.ServiceProvider.GetRequiredService<ApplicationDbContext>();

                    int messagesChannel = dbContext.Channel.Select(c => c.NbMessages).Max();

                    if(messagesChannel > 0)
                    {
                        var popularChannel = dbContext.Channel.Where(c => c.NbMessages >= messagesChannel).ToList();

                        var mostPopularChannel = popularChannel.Select(c => ChatHub.CreateChannelGroupName(c.Id));

                        await _chatHubContext.Clients.Groups(mostPopularChannel).SendAsync("MostPopularChannel", messagesChannel, stoppingToken);
                    }
                }
                await Task.Delay(30 * 1000);
            }
            throw new NotImplementedException();
        }
    }
}
