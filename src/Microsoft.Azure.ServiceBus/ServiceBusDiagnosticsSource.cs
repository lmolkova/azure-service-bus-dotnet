// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace Microsoft.Azure.ServiceBus
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using System.Threading.Tasks;

    internal class ServiceBusDiagnosticsSource
    {
        public const string DiagnosticListenerName = "Microsoft.Azure.ServiceBus";

        public const string ExceptionEventName = "Microsoft.Azure.ServiceBus.Exception";
        public const string ProcessActivityName = "Microsoft.Azure.ServiceBus.Process";
        public const string ProcessActivityStartName = "Microsoft.Azure.ServiceBus.Process.Start";

        public const string BaseActivityName = "Microsoft.Azure.ServiceBus.";

        public const string RequestIdPropertyName = "Request-Id";
        public const string CorrelationContextPropertyName = "Correlation-Context";
        public const string RelatedToTag = "RelatedTo";

        private static readonly DiagnosticListener DiagnosticListener = new DiagnosticListener(DiagnosticListenerName);
        private readonly string entityPath;
        private readonly Uri endpoint;

        public ServiceBusDiagnosticsSource(string entityPath, Uri endpoint)
        {
            this.entityPath = entityPath;
            this.endpoint = endpoint;
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
                    Entity = this.entityPath,
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
                    Messages = messageList,
                    Entity = this.entityPath,
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
            if (DiagnosticListener.IsEnabled(ProcessActivityName, message, this.entityPath))
            {
                var tmpActivity = new Activity(ProcessActivityName);
                tmpActivity.ExtractFrom(message);

                if (DiagnosticListener.IsEnabled(ProcessActivityName, message, tmpActivity))
                {
                    activity = tmpActivity;
                    if (DiagnosticListener.IsEnabled(ProcessActivityStartName))
                    {
                        DiagnosticListener.StartActivity(activity, new
                        {
                            Message = message,
                            Entity = this.entityPath,
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
                    Entity = this.entityPath,
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
                Entity = this.entityPath,
                Endpoint = this.endpoint
            });

            Inject(message);

            return activity;
        }

        internal void ScheduleStop(Activity activity, Message message, DateTimeOffset scheduleEnqueueTimeUtc, TaskStatus? status, long sequenceNumber)
        {
            if (activity != null)
            {
                DiagnosticListener.StopActivity(activity, new
                {
                    Message = message,
                    ScheduleEnqueueTimeUtc = scheduleEnqueueTimeUtc,
                    Entity = this.entityPath,
                    Endpoint = this.endpoint,
                    SequenceNumber = sequenceNumber,
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
                Entity = this.entityPath,
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
                    Entity = this.entityPath,
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
                Entity = this.entityPath,
                Endpoint = this.endpoint
            });
        }

        internal void ReceiveStop(Activity activity, int messageCount, TaskStatus? status, IList<Message> messageList)
        {
            if (activity != null)
            {
                SetRelatedOperations(activity, messageList);

                DiagnosticListener.StopActivity(activity, new
                {
                    MessageCount = messageCount,
                    Entity = this.entityPath,
                    Endpoint = this.endpoint,
                    Status = status ?? TaskStatus.Faulted,
                    Messages = messageList
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
                Entity = this.entityPath,
                Endpoint = this.endpoint
            });
        }

        internal void PeekStop(Activity activity, long fromSequenceNumber, int messageCount, TaskStatus? status, IList<Message> messageList)
        {
            if (activity != null)
            {
                SetRelatedOperations(activity, messageList);

                DiagnosticListener.StopActivity(activity, new
                {
                    FromSequenceNumber = fromSequenceNumber,
                    MessageCount = messageCount,
                    Entity = this.entityPath,
                    Endpoint = this.endpoint,
                    Status = status ?? TaskStatus.Faulted,
                    Messages = messageList
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
                Entity = this.entityPath,
                Endpoint = this.endpoint
            });
        }

        internal void ReceiveDefferedStop(Activity activity, IEnumerable<long> sequenceNumbers, TaskStatus? status, IList<Message> messageList)
        {
            if (activity != null)
            {
                SetRelatedOperations(activity, messageList);

                DiagnosticListener.StopActivity(activity, new
                {
                    SequenceNumbers = sequenceNumbers,
                    Entity = this.entityPath,
                    Endpoint = this.endpoint,
                    Messages = messageList,
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
                Entity = this.entityPath,
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
                    Entity = this.entityPath,
                    Endpoint = this.endpoint,
                    Status = status ?? TaskStatus.Faulted
                });
            }
        }

        #endregion


        #region Dispose

        internal Activity DisposeStart(string operationName, string lockToken)
        {
            return Start(operationName, () => new
            {
                LockToken = lockToken,
                Entity = this.entityPath,
                Endpoint = this.endpoint
            });
        }

        internal void DisposeStop(Activity activity, string lockToken, TaskStatus? status)
        {
            if (activity != null)
            {
                DiagnosticListener.StopActivity(activity, new
                {
                    LockToken = lockToken,
                    Entity = this.entityPath,
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
                Entity = this.entityPath,
                Endpoint = this.endpoint
            });
        }

        internal void RenewLockStop(Activity activity, string lockToken, TaskStatus? status, DateTime lockedUntilUtc)
        {
            if (activity != null)
            {
                DiagnosticListener.StopActivity(activity, new
                {
                    LockToken = lockToken,
                    Entity = this.entityPath,
                    Endpoint = this.endpoint,
                    Status = status ?? TaskStatus.Faulted,
                    LockedUntilUtc = lockedUntilUtc
                });
            }
        }

        #endregion

        internal void ReportException(Exception ex)
        {
            if (DiagnosticListener.IsEnabled(ExceptionEventName))
            {
                DiagnosticListener.Write(ExceptionEventName,
                    new
                    {
                        Exception = ex,
                        Entity = this.entityPath,
                        Endpoint = this.endpoint
                    });
            }
        }

        private Activity Start(string operationName, Func<object> getPayload)
        {
            Activity activity = null;
            string activityName = BaseActivityName + operationName;
            if (DiagnosticListener.IsEnabled(activityName, this.entityPath))
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
            message.UserProperties[RequestIdPropertyName] = id;
            if (correlationContext.Any())
            {
                message.UserProperties[CorrelationContextPropertyName] = correlationContext;
            }
        }

        private void SetRelatedOperations(Activity activity, IList<Message> messageList)
        {
            if (messageList != null)
            {
                string[] relatedTo = new string[messageList.Count];
                for (int i = 0; i < messageList.Count; i++)
                {
                    if (messageList[i].TryExtractId(out string id))
                    {
                        relatedTo[i] = id;
                    }
                }

                activity.AddTag(RelatedToTag, string.Join(",", relatedTo));
            }
        }
    }
}