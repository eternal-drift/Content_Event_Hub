using System.Threading.Channels;
using Confluent.Kafka;
using ContentEventHub.Contracts;

namespace ContentEventHub.Shared;

/// <summary>
/// Bounded-channel + fixed-worker-pool consume loop (ADR 0007). The poll loop writes
/// into a bounded <see cref="Channel{T}"/>; when it's full, the write blocks, which
/// pauses consumption and surfaces as ordinary Kafka consumer lag rather than an
/// internal-only signal. A fixed pool of workers drains the channel concurrently, so
/// completions can arrive out of order; an <see cref="OffsetTracker"/> only exposes the
/// highest contiguously-completed offset per partition, which is committed on a timer.
/// </summary>
public sealed class BackpressureConsumerLoop<TData>(EventConsumer<TData> consumer, BackpressureOptions options)
{
    private readonly OffsetTracker _offsetTracker = new();

    public async Task RunAsync(
        Func<ConsumeResult<string, CloudEventEnvelope<TData>>, Task> processAsync,
        CancellationToken cancellationToken)
    {
        var channel = Channel.CreateBounded<ConsumeResult<string, CloudEventEnvelope<TData>>>(
            new BoundedChannelOptions(options.Capacity) { FullMode = BoundedChannelFullMode.Wait });

        // Task.Run is required here, not just calling the async method directly: Consume()
        // is a blocking synchronous call, so without it PollAsync would run the whole poll
        // loop on the calling thread and never yield to let the worker/commit tasks start.
        var pollTask = Task.Run(() => PollAsync(channel.Writer, cancellationToken), cancellationToken);
        var workerTasks = Enumerable.Range(0, options.ConcurrencyLimit)
            .Select(_ => Task.Run(() => WorkAsync(channel.Reader, processAsync, cancellationToken), cancellationToken))
            .ToArray();
        var commitTask = Task.Run(() => CommitLoopAsync(cancellationToken), cancellationToken);

        try
        {
            await Task.WhenAll(workerTasks.Append(pollTask).Append(commitTask));
        }
        catch (OperationCanceledException)
        {
            // Expected on shutdown.
        }
        finally
        {
            CommitSafeOffsets();
        }
    }

    private async Task PollAsync(
        ChannelWriter<ConsumeResult<string, CloudEventEnvelope<TData>>> writer,
        CancellationToken cancellationToken)
    {
        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                var result = consumer.Consume(TimeSpan.FromMilliseconds(500));
                if (result is null)
                {
                    continue;
                }

                _offsetTracker.MarkStarted(result.TopicPartition, result.Offset.Value);
                await writer.WriteAsync(result, cancellationToken);
            }
        }
        finally
        {
            writer.TryComplete();
        }
    }

    private async Task WorkAsync(
        ChannelReader<ConsumeResult<string, CloudEventEnvelope<TData>>> reader,
        Func<ConsumeResult<string, CloudEventEnvelope<TData>>, Task> processAsync,
        CancellationToken cancellationToken)
    {
        await foreach (var result in reader.ReadAllAsync(cancellationToken))
        {
            await processAsync(result);
            _offsetTracker.MarkCompleted(result.TopicPartition, result.Offset.Value);
        }
    }

    private async Task CommitLoopAsync(CancellationToken cancellationToken)
    {
        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                await Task.Delay(options.CommitIntervalMs, cancellationToken);
                CommitSafeOffsets();
            }
        }
        catch (OperationCanceledException)
        {
            // Expected on shutdown.
        }
    }

    private void CommitSafeOffsets()
    {
        var safe = _offsetTracker.DrainSafeOffsets();
        if (safe.Count == 0)
        {
            return;
        }

        consumer.Commit(safe.Select(kvp => new TopicPartitionOffset(kvp.Key, kvp.Value)));
    }
}
