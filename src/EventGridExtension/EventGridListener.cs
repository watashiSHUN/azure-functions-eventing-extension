using Microsoft.Azure.WebJobs.Host.Executors;
using Microsoft.Azure.WebJobs.Host.Queues;
using Newtonsoft.Json.Linq;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Microsoft.Azure.WebJobs.Extensions.EventGrid
{
    public class EventGridListener : Microsoft.Azure.WebJobs.Host.Listeners.IListener
    {
        private readonly string _functionId; // TODO property
        private SharedQueueHandler _sqHandler;
        private EventGridMessageHandler _messageHandler;

        private EventGridExtensionConfig _listenersStore;
        private readonly string _functionName;

        // TODO pass the context
        public EventGridListener(ITriggeredFunctionExecutor executor, string functionId, SharedQueueHandler sqHandler, EventGridExtensionConfig listenersStore, string functionName)
        {
            _functionId = functionId;
            _sqHandler = sqHandler;
            _listenersStore = listenersStore;
            _functionName = functionName;
            _messageHandler = new EventGridMessageHandler(executor);
        }

        public async Task ExecuteBatch(List<JObject> inputs, CancellationToken cancellationToken)
        {
            foreach (var input in inputs)
            {
                await _messageHandler.TryExecuteAsync(input, cancellationToken);
            }
        }

        public async Task EnqueueBatch(List<JObject> inputs, CancellationToken cancellationToken)
        {
            foreach (var input in inputs)
            {
                await _sqHandler.EnqueueAsync(input, _functionId, cancellationToken);
            }
        }

        public Task StartAsync(CancellationToken cancellationToken)
        {
            // register call back
            _sqHandler.RegisterHandler(_functionId, _messageHandler);
            _listenersStore.AddListener(_functionName, this);
            return Task.FromResult(true);
        }

        public Task StopAsync(CancellationToken cancellationToken)
        {
            // calling order stop -> cancel -> dispose
            _sqHandler.UnregisterHandler(_functionId);
            _listenersStore.RemoveListener(_functionName);
            return Task.FromResult(true);
        }

        public void Dispose()
        {
            // TODO unsubscribe 
        }

        public void Cancel()
        {
            // TODO cancel any outstanding tasks initiated by this listener
        }

        private class EventGridMessageHandler : MessageHandler
        {
            private readonly ITriggeredFunctionExecutor _executor;

            internal EventGridMessageHandler(ITriggeredFunctionExecutor executor)
            {
                _executor = executor;
            }

            public override Task<FunctionResult> TryExecuteAsync(JObject data, CancellationToken cancellationToken)
            {
                EventGridEvent eventInMessage = data.ToObject<EventGridEvent>();
                TriggeredFunctionData triggerData = new TriggeredFunctionData
                {
                    TriggerValue = eventInMessage
                };
                return _executor.TryExecuteAsync(triggerData, cancellationToken);
            }
        }
    }
}
