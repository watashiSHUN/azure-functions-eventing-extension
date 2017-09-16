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
        private readonly DispatchProcessor _dispatchProcessor;
        private readonly EventGridMessageHandler _messageHandler;

        private readonly EventGridExtensionConfig _listenersStore;
        private readonly string _functionName;

        // TODO pass the context
        public EventGridListener(ITriggeredFunctionExecutor executor, DispatchProcessorFactory factory, EventGridExtensionConfig listenersStore, string functionName)
        {
            _listenersStore = listenersStore;
            _functionName = functionName;
            _messageHandler = new EventGridMessageHandler(executor);
            // register call
            _dispatchProcessor = factory.CreateDispatchProcessor(_messageHandler);
        }

        public async Task ExecuteBatch(List<JObject> inputs, CancellationToken cancellationToken)
        {
            foreach (var input in inputs)
            {
                await _messageHandler.TryExecuteAsync(input, cancellationToken);
            }
        }

        public async Task DispatchBatch(List<JObject> inputs, CancellationToken cancellationToken)
        {
            foreach (var input in inputs)
            {
                await _dispatchProcessor.ProcessAsync(input, cancellationToken);
            }
        }

        public Task StartAsync(CancellationToken cancellationToken)
        {
            _listenersStore.AddListener(_functionName, this);
            return Task.FromResult(true);
        }

        public Task StopAsync(CancellationToken cancellationToken)
        {
            // calling order stop -> cancel -> dispose
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
