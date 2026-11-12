namespace ContentEventHub.DlqOperator;

public enum DlqOperationOutcome
{
    Success,
    NotFound,
    AlreadyResolved,
}

/// <summary>Non-generic facade so Program.cs can route by topic name without generics leaking out.</summary>
public interface IDlqTopicHandler
{
    string OriginalTopic { get; }
    string DlqTopic { get; }
    Task RunAsync(CancellationToken cancellationToken);
    IReadOnlyList<DlqEntry> List();
    Task<DlqOperationOutcome> ReplayAsync(string id, CancellationToken cancellationToken);
    DlqOperationOutcome Discard(string id);
}
