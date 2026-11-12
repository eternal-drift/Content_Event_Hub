using Confluent.Kafka;

namespace ContentEventHub.Shared;

/// <summary>
/// Tracks which offsets are safe to commit per partition when a worker pool completes
/// messages out of order (ADR 0007). A Kafka commit is a single watermark per
/// partition, not a sparse set, so offset N+1 is only safe once every offset up to and
/// including N has completed — even if a later offset finished first.
/// </summary>
public sealed class OffsetTracker
{
    private readonly object _gate = new();
    private readonly Dictionary<TopicPartition, long> _nextExpected = new();
    private readonly Dictionary<TopicPartition, HashSet<long>> _completedAheadOfExpected = new();
    private readonly Dictionary<TopicPartition, long> _safeToCommit = new();

    public void MarkStarted(TopicPartition partition, long offset)
    {
        lock (_gate)
        {
            if (!_nextExpected.ContainsKey(partition))
            {
                _nextExpected[partition] = offset;
                _completedAheadOfExpected[partition] = [];
            }
        }
    }

    public void MarkCompleted(TopicPartition partition, long offset)
    {
        lock (_gate)
        {
            var pending = _completedAheadOfExpected[partition];
            pending.Add(offset);

            var next = _nextExpected[partition];
            while (pending.Remove(next))
            {
                next++;
            }

            _nextExpected[partition] = next;
            _safeToCommit[partition] = next;
        }
    }

    /// <summary>Returns the highest safe-to-commit offset per partition since the last drain, then clears it.</summary>
    public IReadOnlyDictionary<TopicPartition, long> DrainSafeOffsets()
    {
        lock (_gate)
        {
            if (_safeToCommit.Count == 0)
            {
                return new Dictionary<TopicPartition, long>();
            }

            var snapshot = new Dictionary<TopicPartition, long>(_safeToCommit);
            _safeToCommit.Clear();
            return snapshot;
        }
    }
}
