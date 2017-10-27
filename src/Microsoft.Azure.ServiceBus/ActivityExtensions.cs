// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Diagnostics;

namespace Microsoft.Azure.ServiceBus
{
    public static class MessageExtensions
    {
        /// <summary>
        /// Gets trace context from <see cref="Message"/> and stores it in <see cref="Activity"/>
        /// </summary>
        /// <returns>Activity containing trace context</returns>
        public static Activity ExtractActivity(this Message message, string activityName = null)
        {
            if (message == null)
            {
                throw new ArgumentNullException(nameof(message));
            }

            if (activityName == null)
            {
                activityName = ServiceBusDiagnosticSource.ProcessActivityName;
            }

            var activity = new Activity(activityName);

            if (TryExtractId(message, out string id))
            {
                activity.SetParentId(id);

                if (message.TryExtractContext(out KeyValuePair<string, string>[] ctx))
                {
                    foreach (var kvp in ctx)
                    {
                        activity.AddBaggage(kvp.Key, kvp.Value);
                    }
                }
            }

            return activity;
        }

        internal static bool TryExtractId(this Message message, out string id)
        {
            id = null;
            if (message.UserProperties.TryGetValue(ServiceBusDiagnosticSource.RequestIdPropertyName,
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

        internal static bool TryExtractContext(this Message message, out KeyValuePair<string, string>[] context)
        {
            context = null;
            if (message.UserProperties.TryGetValue(ServiceBusDiagnosticSource.CorrelationContextPropertyName,
                out object ctxObj))
            {
                string ctxStr = ctxObj as string;
                if (ctxStr == null)
                {
                    return false;
                }

                var ctxList = ctxStr.Split(',');
                if (ctxList.Length == 0)
                {
                    return false;
                }

                context = new KeyValuePair<string,string>[ctxList.Length];
                for (int i = 0; i < ctxList.Length; i ++)
                {
                    var kvp = ctxList[i].Split('=');
                    if (kvp.Length == 2)
                    {
                        context[i] = new KeyValuePair<string,string>(kvp[0],kvp[1]);
                    }
                }

                return true;
            }

            return false;
        }
    }
}