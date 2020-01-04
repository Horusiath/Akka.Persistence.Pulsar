#region copyright
// -----------------------------------------------------------------------
//  <copyright file="AsyncEnumerableSource.cs" company="Akka.NET Project">
//      Copyright (C) 2009-2020 Lightbend Inc. <http://www.lightbend.com>
//      Copyright (C) 2013-2020 .NET Foundation <https://github.com/akkadotnet/akka.net>
//  </copyright>
// -----------------------------------------------------------------------
#endregion

using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using Akka.Streams;
using Akka.Streams.Stage;

namespace Akka.Persistence.Pulsar
{
    public sealed class AsyncEnumerableSourceStage<T> : GraphStage<SourceShape<T>>
    {
        #region logic

        private sealed class Logic : OutGraphStageLogic
        {
            private readonly IAsyncEnumerator<T> enumerator;
            private readonly Outlet<T> outlet;
            private readonly Action<T> onSuccess;
            private readonly Action<Exception> onFailure;
            private readonly Action onComplete;
            private readonly Action<Task<bool>> handleContinuation;

            public Logic(SourceShape<T> shape, IAsyncEnumerator<T> enumerator) : base(shape)
            {
                this.enumerator = enumerator;
                this.outlet = shape.Outlet;
                this.onSuccess = GetAsyncCallback<T>(this.OnSuccess);
                this.onFailure = GetAsyncCallback<Exception>(this.OnFailure);
                this.onComplete = GetAsyncCallback(this.OnComplete);
                this.handleContinuation = task =>
                {
                    if (task.IsFaulted) this.onFailure(task.Exception);
                    else if (task.IsCanceled) this.onFailure(new TaskCanceledException(task));
                    else if (task.Result) this.onSuccess(enumerator.Current);
                    else this.onComplete();
                };
                
                this.SetHandler(this.outlet, this);
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            private void OnComplete() => this.CompleteStage();

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            private void OnFailure(Exception exception) => FailStage(exception);

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            private void OnSuccess(T element) => Push(this.outlet, element);

            public override void OnPull()
            {
                var vtask = enumerator.MoveNextAsync();
                if (vtask.IsCompletedSuccessfully)
                {
                    // short circuit
                    if (vtask.Result)
                    {
                        Push(this.outlet, enumerator.Current);
                    }
                    else
                    {
                        CompleteStage();
                    }
                }
                else
                {
                    vtask.AsTask().ContinueWith(this.handleContinuation);
                }
            }

            public override void OnDownstreamFinish()
            {
                var vtask = this.enumerator.DisposeAsync();
                if (vtask.IsCompletedSuccessfully)
                {
                    this.CompleteStage();
                }
                else
                {
                    vtask.GetAwaiter().OnCompleted(this.onComplete);
                }
            }
        }

        #endregion
        
        private readonly Outlet<T> outlet = new Outlet<T>("asyncEnumerable.out");
        private readonly IAsyncEnumerable<T> asyncEnumerable;

        public AsyncEnumerableSourceStage(IAsyncEnumerable<T> asyncEnumerable)
        {
            this.asyncEnumerable = asyncEnumerable;
            Shape = new SourceShape<T>(outlet);
        }

        public override SourceShape<T> Shape { get; } 
        protected override GraphStageLogic CreateLogic(Attributes inheritedAttributes) => new Logic(this.Shape, this.asyncEnumerable.GetAsyncEnumerator());
    }
}