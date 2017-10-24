using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Azure.Amqp.Framing;

namespace Microsoft.Azure.ServiceBus
{
    internal static class DiagnosticsLoggingStrings
    {
        public const string DiagnosticListenerName = "Microsoft.Azure.ServiceBus";

        public const string ExceptionEventName = "Microsoft.Azure.ServiceBus.Exception";
        public const string ProcessActivityName = "Microsoft.Azure.ServiceBus.Process";
        public const string ProcessActivityStartName = "Microsoft.Azure.ServiceBus.Process.Start";

        public const string BaseActivityName = "Microsoft.Azure.ServiceBus.";

        public const string RequestIdPropertyName = "Request-Id";
        public const string CorrelationContextPropertyName = "Correlation-Context";
    }

    internal class ServiceBusDiagnosticsSource
    {
        private static readonly DiagnosticListener DiagnosticListener = new DiagnosticListener(DiagnosticsLoggingStrings.DiagnosticListenerName);
        private readonly string queueName;
        private readonly string endpoint;
        private readonly string clientId;

        public ServiceBusDiagnosticsSource(string queueName, string endpoint, string clientId)
        {
            this.queueName = queueName;
            this.endpoint = endpoint;
            this.clientId = clientId;
        }

        public static bool IsEnabled()
        {
            return DiagnosticListener.IsEnabled();
        }

        #region Send

        internal Activity SendStart(IList<Message> messageList)
        {
            Activity activity = Start("Send", () =>
                new
                {
                    Messages = messageList,
                    QueueName = this.queueName,
                    ClientId = this.clientId,
                    Endpoint = this.endpoint
                });
            
            Inject(messageList);

            return activity;
        }

        internal void SendStop(Activity activity, IList<Message> messageList, TaskStatus? status)
        {
            if (activity != null)
            {
                DiagnosticListener.StopActivity(activity, new
                {
                    MessageList = messageList,
                    QueueName = this.queueName,
                    ClientId = this.clientId,
                    Endpoint = this.endpoint,
                    Status = status ?? TaskStatus.Faulted
                });
            }
        }
        #endregion

        #region Process

        internal Activity ProcessStart(Message message)
        {
            Activity activity = null;
            if (DiagnosticListener.IsEnabled(DiagnosticsLoggingStrings.ProcessActivityName, message, this.queueName))
            {
                var tmpActivity = new Activity(DiagnosticsLoggingStrings.ProcessActivityName);
                TryExtract(message, tmpActivity);

                if (DiagnosticListener.IsEnabled(DiagnosticsLoggingStrings.ProcessActivityName, message, tmpActivity))
                {
                    activity = tmpActivity;
                    if (DiagnosticListener.IsEnabled(DiagnosticsLoggingStrings.ProcessActivityStartName))
                    {
                        DiagnosticListener.StartActivity(activity, new
                        {
                            Message = message,
                            QueueName = this.queueName,
                            ClientId = this.clientId,
                            Endpoint = this.endpoint
                        });
                    }
                    else
                    {
                        activity.Start();
                    }
                }
            }
            return activity;
        }

        internal void ProcessStop(Activity activity, Message message, TaskStatus? status)
        {
            if (activity != null)
            {
                DiagnosticListener.StopActivity(activity, new
                {
                    Message = message,
                    QueueName = this.queueName,
                    ClientId = this.clientId,
                    Endpoint = this.endpoint,
                    Status = status ?? TaskStatus.Faulted
                });
            }
        }

        #endregion

        #region Schedule

        internal Activity ScheduleStart(Message message, DateTimeOffset scheduleEnqueueTimeUtc)
        {
            Activity activity = Start("Schedule", () => new
            {
                Message = message,
                ScheduleEnqueueTimeUtc = scheduleEnqueueTimeUtc,
                QueueName = this.queueName,
                ClientId = this.clientId,
                Endpoint = this.endpoint
            });

            Inject(message);

            return activity;
        }

        internal void ScheduleStop(Activity activity, Message message, DateTimeOffset scheduleEnqueueTimeUtc, TaskStatus? status)
        {
            if (activity != null)
            {
                DiagnosticListener.StopActivity(activity, new
                {
                    Message = message,
                    ScheduleEnqueueTimeUtc = scheduleEnqueueTimeUtc,
                    QueueName = this.queueName,
                    ClientId = this.clientId,
                    Endpoint = this.endpoint,
                    Status = status ?? TaskStatus.Faulted
                });
            }
        }

        #endregion

        #region Cancel

        internal Activity CancelStart(long sequenceNumber)
        {
            return Start("Cancel", () => new
            {
                SequenceNumber = sequenceNumber,
                QueueName = this.queueName,
                ClientId = this.clientId,
                Endpoint = this.endpoint
            });
        }

        internal void CancelStop(Activity activity, long sequenceNumber, TaskStatus? status)
        {
            if (activity != null)
            {
                DiagnosticListener.StopActivity(activity, new
                {
                    SequenceNumber = sequenceNumber,
                    QueueName = this.queueName,
                    ClientId = this.clientId,
                    Endpoint = this.endpoint,
                    Status = status ?? TaskStatus.Faulted
                });
            }
        }

        #endregion

        #region Receive

        internal Activity ReceiveStart(int messageCount)
        {
            return Start("Receive", () => new
            {
                MessageCount = messageCount,
                QueueName = this.queueName,
                ClientId = this.clientId,
                Endpoint = this.endpoint
            });
        }

        internal void ReceiveStop(Activity activity, int messageCount, TaskStatus? status)
        {
            if (activity != null)
            {
                DiagnosticListener.StopActivity(activity, new
                {
                    MessageCount = messageCount,
                    QueueName = this.queueName,
                    ClientId = this.clientId,
                    Endpoint = this.endpoint,
                    Status = status ?? TaskStatus.Faulted
                });
            }
        }

        #endregion

        #region Peek

        internal Activity PeekStart(long fromSequenceNumber, int messageCount)
        {
            return Start("Peek", () => new
            {
                FromSequenceNumber = fromSequenceNumber,
                MessageCount = messageCount,
                QueueName = this.queueName,
                ClientId = this.clientId,
                Endpoint = this.endpoint
            });
        }

        internal void PeekStop(Activity activity, long fromSequenceNumber, int messageCount, TaskStatus? status)
        {
            if (activity != null)
            {
                DiagnosticListener.StopActivity(activity, new
                {
                    FromSequenceNumber = fromSequenceNumber,
                    MmessageCount = messageCount,
                    QueueName = this.queueName,
                    ClientId = this.clientId,
                    Endpoint = this.endpoint,
                    Status = status ?? TaskStatus.Faulted
                });
            }
        }

        #endregion

        #region ReceiveDeffered

        internal Activity ReceiveDefferedStart(IEnumerable<long> sequenceNumbers)
        {
            return Start("ReceiveDeffered", () => new
            {
                SequenceNumbers = sequenceNumbers,
                QueueName = this.queueName,
                ClientId = this.clientId,
                Endpoint = this.endpoint
            });
        }

        internal void ReceiveDefferedStop(Activity activity, IEnumerable<long> sequenceNumbers, TaskStatus? status)
        {
            if (activity != null)
            {
                DiagnosticListener.StopActivity(activity, new
                {
                    SequenceNumbers = sequenceNumbers,
                    QueueName = this.queueName,
                    ClientId = this.clientId,
                    Endpoint = this.endpoint,
                    Status = status ?? TaskStatus.Faulted
                });
            }
        }

        #endregion

        #region  Complete

        internal Activity CompleteStart(IList<string> lockTokens, IDictionary<string, object> propertiesToModify)
        {
            return Start("Complete", () => new
            {
                LockTokens = lockTokens,
                PropertiesToModify = propertiesToModify,
                QueueName = this.queueName,
                ClientId = this.clientId,
                Endpoint = this.endpoint
            });
        }

        internal void CompleteStop(Activity activity, IList<string> lockTokens, IDictionary<string, object> propertiesToModify, TaskStatus? status)
        {
            if (activity != null)
            {
                DiagnosticListener.StopActivity(activity, new
                {
                    LockTokens = lockTokens,
                    PropertiesToModify = propertiesToModify,
                    QueueName = this.queueName,
                    ClientId = this.clientId,
                    Endpoint = this.endpoint,
                    Status = status ?? TaskStatus.Faulted
                });
            }
        }

        #endregion

        #region Abandon

        internal Activity AbandonStart(string lockToken, IDictionary<string, object> propertiesToModify)
        {
            return Start("Abandon", () => new
            {
                LockToken = lockToken,
                PropertiesToModify = propertiesToModify,
                QueueName = this.queueName,
                ClientId = this.clientId,
                Endpoint = this.endpoint
            });
        }

        internal void AbandonStop(Activity activity, string lockToken, IDictionary<string, object> propertiesToModify, TaskStatus? status)
        {
            if (activity != null)
            {
                DiagnosticListener.StopActivity(activity, new
                {
                    LockToken = lockToken,
                    PropertiesToModify = propertiesToModify,
                    QueueName = this.queueName,
                    ClientId = this.clientId,
                    Endpoint = this.endpoint,
                    Status = status ?? TaskStatus.Faulted
                });
            }
        }

        #endregion

        #region Defer

        internal Activity DeferStart(string lockToken, IDictionary<string, object> propertiesToModify)
        {
            return Start("Defer", () => new
            {
                LockToken = lockToken,
                PropertiesToModify = propertiesToModify,
                QueueName = this.queueName,
                ClientId = this.clientId,
                Endpoint = this.endpoint
            });
        }

        internal void DeferStop(Activity activity, string lockToken, IDictionary<string, object> propertiesToModify, TaskStatus? status)
        {
            if (activity != null)
            {
                DiagnosticListener.StopActivity(activity, new
                {
                    LockToken = lockToken,
                    PropertiesToModify = propertiesToModify,
                    QueueName = this.queueName,
                    ClientId = this.clientId,
                    Endpoint = this.endpoint,
                    Status = status ?? TaskStatus.Faulted
                });
            }
        }

        #endregion

        #region DeadLetter

        internal Activity DeadLetterStart(string lockToken, IDictionary<string, object> propertiesToModify, string deadLetterReason, string deadLetterDescription)
        {
            return Start("DeadLetter", () => new
            {
                LockToken = lockToken,
                PropertiesToModify = propertiesToModify,
                DeadLetterReason = deadLetterReason,
                DeadLetterDescription = deadLetterDescription,
                QueueName = this.queueName,
                ClientId = this.clientId,
                Endpoint = this.endpoint
            });
        }

        internal void DeadLetterStop(Activity activity, string lockToken, IDictionary<string, object> propertiesToModify, string deadLetterReason, string deadLetterDescription, TaskStatus? status)
        {
            if (activity != null)
            {
                DiagnosticListener.StopActivity(activity, new
                {
                    LockToken = lockToken,
                    PropertiesToModify = propertiesToModify,
                    DeadLetterReason = deadLetterReason,
                    DeadLetterDescription = deadLetterDescription,
                    QueueName = this.queueName,
                    ClientId = this.clientId,
                    Endpoint = this.endpoint,
                    Status = status ?? TaskStatus.Faulted
                });
            }
        }

        #endregion

        #region RenewLock

        internal Activity RenewLockStart(string lockToken)
        {
            return Start("RenewLock", () => new
            {
                LockToken = lockToken,
                QueueName = this.queueName,
                ClientId = this.clientId,
                Endpoint = this.endpoint
            });
        }

        internal void RenewLockStop(Activity activity, string lockToken, TaskStatus? status)
        {
            if (activity != null)
            {
                DiagnosticListener.StopActivity(activity, new
                {
                    LockToken = lockToken,
                    QueueName = this.queueName,
                    ClientId = this.clientId,
                    Endpoint = this.endpoint,
                    Status = status ?? TaskStatus.Faulted
                });
            }
        }

        #endregion

        internal void ReportException(Exception ex)
        {
            if (DiagnosticListener.IsEnabled(DiagnosticsLoggingStrings.ExceptionEventName))
            {
                DiagnosticListener.Write(DiagnosticsLoggingStrings.ExceptionEventName,
                    new
                    {
                        Exception = ex,
                        QueueName = this.queueName,
                        ClientId = this.clientId,
                        Endpoint = this.endpoint
                    });
            }
        }

        private Activity Start(string operationName, Func<object> getPayload)
        {
            Activity activity = null;
            string activityName = DiagnosticsLoggingStrings.BaseActivityName + operationName;
            if (DiagnosticListener.IsEnabled(activityName, queueName))
            {
                activity = new Activity(activityName);

                if (DiagnosticListener.IsEnabled(activityName + ".Start"))
                {
                    DiagnosticListener.StartActivity(activity, getPayload());
                }
                else
                {
                    activity.Start();
                }
            }

            return activity;
        }

        private void Inject(IList<Message> messageList)
        {
            var currentActivity = Activity.Current;
            if (currentActivity != null)
            {
                var correlationContext = currentActivity.Baggage.ToArray();

                foreach (var message in messageList)
                {
                    Inject(message, currentActivity.Id, correlationContext);
                }
            }
        }

        private void Inject(Message message)
        {
            var currentActivity = Activity.Current;
            if (currentActivity != null)
            {
                Inject(message, currentActivity.Id, currentActivity.Baggage.ToArray());
            }
        }

        private void Inject(Message message, string id, IList<KeyValuePair<string, string>> correlationContext)
        {
            message.UserProperties[DiagnosticsLoggingStrings.RequestIdPropertyName] = id;
            if (correlationContext.Any())
            {
                message.UserProperties[DiagnosticsLoggingStrings.CorrelationContextPropertyName] = correlationContext;
            }
        }

        private bool TryExtract(Message message, Activity activity)
        {
            if (message.UserProperties.TryGetValue(DiagnosticsLoggingStrings.RequestIdPropertyName,
                out object requestId))
            {
                var id = (string)requestId;
                if (id == null)
                {
                    return false;
                }

                activity.SetParentId(id);

                if (message.UserProperties.TryGetValue(DiagnosticsLoggingStrings.CorrelationContextPropertyName,
                    out object ctxObj))
                {
                    var ctx = (KeyValuePair<string, string>[])ctxObj;
                    if (ctx != null)
                    {
                        foreach (var kvp in ctx)
                        {
                            activity.AddBaggage(kvp.Key, kvp.Value);
                        }
                    }
                }

                return true;
            }

            return false;
        }
    }
}