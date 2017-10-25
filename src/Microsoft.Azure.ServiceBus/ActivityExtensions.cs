// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace System.Diagnostics
{
    using System.Collections.Generic;
    using Microsoft.Azure.ServiceBus;

    public static class ActivityExtensions
    {
        /// <summary>
        /// Gets trace context from <see cref="Message"/> and stores it in <see cref="Activity"/>
        /// </summary>
        /// <param name="message">Message to extract trace context from</param>
        /// <param name="activity">Activity to be updated with trace context, Activity must be created by the caller and must not be started yet.</param>
        /// <returns>Activity for convenient chaining</returns>
        public static Activity ExtractFrom(this Activity activity, Message message)
        {
            if (message == null)
            {
                throw new ArgumentNullException(nameof(message));
            }

            if (activity == null)
            {
                throw new ArgumentNullException(nameof(activity));
            }

            if (activity.Id != null)
            {
                throw new ArgumentException("Cannot update Activity that has beed started");
            }

            if (TryExtractId(message, out string id))
            {
                activity.SetParentId(id);

                if (message.UserProperties.TryGetValue(ServiceBusDiagnosticsSource.CorrelationContextPropertyName,
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
            }

            return activity;
        }

        internal static bool TryExtractId(this Message message, out string id)
        {
            id = null;
            if (message.UserProperties.TryGetValue(ServiceBusDiagnosticsSource.RequestIdPropertyName,
                out object requestId))
            {
                id = requestId as string;
                if (id != null)
                {
                    return true;
                }
            }

            return false;
        }
    }
}