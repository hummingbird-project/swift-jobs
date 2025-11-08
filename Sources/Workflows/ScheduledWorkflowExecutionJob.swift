import Jobs
import Logging
import NIOCore

#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif

/// Job parameters for scheduled workflow execution
internal struct ScheduledWorkflowExecutionJob: JobParameters {
    public static let jobName = "ScheduledWorkflowExecution"

    let workflowTypeName: String
    let inputBuffer: ByteBuffer
    let scheduleId: String?

    internal init<W: WorkflowProtocol>(
        workflowType: W.Type,
        input: W.Input,
        scheduleId: String? = nil,
        options: WorkflowOptions = .init()
    ) throws {
        self.workflowTypeName = W.workflowName
        self.inputBuffer = try JSONEncoder().encodeAsByteBuffer(input, allocator: ByteBufferAllocator())
        self.scheduleId = scheduleId
    }
}
