<p align="center">
<a href="https://swift.org">
  <img src="https://img.shields.io/badge/swift-6.0-brightgreen.svg"/>
</a>
<a href="https://github.com/hummingbird-project/swift-jobs/actions?query=workflow%3ACI">
  <img src="https://github.com/hummingbird-project/swift-jobs/actions/workflows/ci.yml/badge.svg?branch=main"/>
</a>
<a href="https://discord.gg/7ME3nZ7mP2">
  <img src="https://img.shields.io/badge/chat-discord-brightgreen.svg"/>
</a>
</p>

# Jobs

Job queue for processing workloads asynchronously across multiple nodes.
- Queue, pause, resume, cancel jobs
- Queue delayed jobs
- Job processor can be run on multiple nodes
- Multiple drivers available (using [Valkey/Redis](https://github.com/hummingbird-project/swift-jobs-valkey), [Postgres](https://github.com/hummingbird-project/swift-jobs-postgres)
- Graceful handling of SIGTERM, SIGINT
- Job scheduler for crontab style triggering of jobs
- Customizable retry support, with default implementation using exponential jitter
- Middleware support to allow for additional customisation
- Support for metrics and distributed tracing

## Register a Job

```swift
// create job queue stored in local memory, that can run four jobs concurrently
let jobQueue = JobQueue(
    .memory,
    logger: logger
)
// Define email job parameters. Its job name has to be unique
struct SendEmailJobParameters: JobParameters {
    static let jobName = "SendEmail"
    let to: String
    let subject: String
    let body: String
}
// Register jobs with job queue
jobQueue.registerJob(parameters: SendEmailJobParameters.self) { parameters, context in
    try await myEmailService.sendEmail(
        to: parameters.to,
        subject: parameters.subject,
        body: parameters.body
    )
}
// Push instance of job onto queue
jobQueue.push(SendEmailJobParameters(
    to: "Ellen",
    subject:"Hello",
    body: "Hi there!"
))
```

## Process Jobs

You can create a `JobQueueProcessor` to process jobs added to yout `JobQueue`. `JobQueueProcessor` conforms to `Service` and can be used with `ServiceGroup` from `ServiceLifecycle`.

```swift
let serviceGroup = ServiceGroup(
    configuration: .init(
        services: [jobQueue.processor(options: .init(numWorkers: 4))],
        gracefulShutdownSignals: [.sigterm, .sigint],
        logger: Logger(label: "JobQueueService")
    )
)
try await serviceGroup.run()
```

Or it can be added as a service attached to a Hummingbird application

```swift
let app = Application(router: router, services: [jobQueue.processor(options: .init(numWorkers: 4))])
```

When the `JobQueueProcessor` service is running it processes jobs as they appear on the queue. The `numWorkers` field in the options initializer indicates how many jobs it will run concurrently.

## Workflows

Workflows provide a way to coordinate multiple jobs with complex execution patterns, including parallel execution, conditional logic, and error handling.

```swift
// Define activity parameters
struct ValidateOrderActivity: ActivityParameters {

    struct Input: Codable, Sendable {
        let orderId: String
        let customerId: String
        let amount: Double
    }

    struct Output: Codable, Sendable {
        let result: Bool
    }
}

struct ProcessPaymentActivity: ActivityParameters {

    struct Input: Codable, Sendable {
        let customerId: String
        let amount: Double
    }

    struct Output: Codable, Sendable {
        let paymentId: String
    }
}

struct UpdateOrderStatusActivity: ActivityParameters {

    struct Input: Codable, Sendable {
        let orderId: String
        let status: String
    }

    struct Output: Codable, Sendable {}
}

// Define a workflow that processes orders
struct OrderProcessingWorkflow: WorkflowProtocol {

    struct Input: Codable, Sendable {
        let orderId: String
        let customerId: String
        let amount: Double
    }

    struct Output: Codable, Sendable {
        let orderId: String
        let paymentId: String
    }

    func run(input: Input, context: WorkflowExecutionContext) async throws -> Output {
        // Step 1: Validate the order
        let isValid = try await context.executeActivity(
            ValidateOrderActivity.self,
            input: ValidateOrderActivity.Input(
                orderId: input.orderId,
                customerId: input.customerId,
                amount: input.amount
            )
        )

        guard isValid.result else {
            throw WorkflowError.validationFailed("Invalid order")
        }

        // Step 2: Process payment
        let payment = try await context.executeActivity(
            ProcessPaymentActivity.self,
            input: ProcessPaymentActivity.Input(
                customerId: input.customerId,
                amount: input.amount
            )
        )

        // Step 3: Update order status
        try await context.executeActivity(
            UpdateOrderStatusActivity.self,
            input: UpdateOrderStatusActivity.Input(
                orderId: input.orderId,
                status: "completed"
            )
        )

        return Output(
            orderId: input.orderId,
            paymentId: payment.paymentId
        )
    }
}

// Register activities and workflow with the engine
struct OrderActivityContainer: ActivityContainer {
    func registerActivities(with registry: ActivityRegistry) {
        registry.registerActivity(ValidateOrderActivity.self) { input in
            // Validation logic here
            return ValidateOrderActivity.Output(result: true)
        }

        registry.registerActivity(ProcessPaymentActivity.self) { input in
            // Payment processing logic here
            return ProcessPaymentActivity.Output(paymentId: UUID().uuidString)
        }

        registry.registerActivity(UpdateOrderStatusActivity.self) { input in
            // Status update logic here
            return UpdateOrderStatusActivity.Output()
        }
    }
}

// Create workflow engine
let workflowEngine = WorkflowEngine(
    jobQueue: jobQueue,
    logger: logger,
    activities: [OrderActivityContainer()]
)

// Register the workflow
workflowEngine.registerWorkflow(OrderProcessingWorkflow.self)

// Start a workflow execution
let workflowId = try await workflowEngine.startWorkflow(
    OrderProcessingWorkflow.self,
    input: OrderProcessingWorkflow.Input(
        orderId: "ORDER-123",
        customerId: "CUSTOMER-456",
        amount: 99.99
    )
)

// Monitor workflow progress
let status = try await workflowEngine.getWorkflowStatus(workflowId)
print("Workflow status: \(status.status)")
```

### Scheduled Workflows

You can schedule workflows to run at specific times or intervals:

```swift
// Schedule a daily report workflow
let jobSchedule = try workflowEngine.scheduleWorkflow(
    DailyReportWorkflow.self,
    input: DailyReportWorkflow.Input(reportType: "sales"),
    schedule: .daily(hour: 2, minute: 0), // Run at 2:00 AM daily
    scheduleId: "daily-sales-report"
)

let schedulerService = await workflowEngine.createSchedulerService(with: jobSchedule)

// Add scheduler to service group
let serviceGroup = ServiceGroup(
    configuration: .init(
        services: [jobQueue.processor(options: .init(numWorkers: 4)), schedulerService],
        logger: logger
    )
)
```

## Documentation

You can find documentation for Jobs [here](https://docs.hummingbird.codes/2.0/documentation/jobs). The [hummingbird-examples](https://github.com/hummingbird-project/hummingbird-examples) repository has a number of examples of different uses of the library.
