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
let jobService = JobService(
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
jobService.registerJob(parameters: SendEmailJobParameters.self) { parameters, context in
    try await myEmailService.sendEmail(
        to: parameters.to, 
        subject: parameters.subject, 
        body: parameters.body
    )
}
// Push instance of job onto queue
jobService.push(SendEmailJobParameters(
    to: "Ellen", 
    subject:"Hello", 
    body: "Hi there!"
))
```

## Processing Jobs

`JobService` conforms to `Service` and can be used with `ServiceGroup` from `ServiceLifecycle`. Internally this will create a `JobQueueProcessor` to process jobs added to your `JobService`

```swift
let serviceGroup = ServiceGroup(
    configuration: .init(
        services: [jobService],
        gracefulShutdownSignals: [.sigterm, .sigint],
        logger: Logger(label: "JobQueueService")
    )
)
try await serviceGroup.run()
```

Or it can be added as a service attached to a Hummingbird application

```swift
let app = Application(router: router, services: [jobService])
```

When the `JobQueueProcessor` service is running it executes jobs as they appear on the queue. The `numWorkers` field in the options initializer indicates how many jobs it will run concurrently.

## Scheduling Jobs

`JobService` also include a job scheduler that will run jobs at regular intervals.

```swift
jobService.addScheduledJob(CleanupDatabaseJob(), schedule: .weekly(day: .sunday, hour: 4))
// the scheduler also accepts crontab strings and will support most combinations
jobService.addScheduledJob(CleanupDatabaseJob(), schedule: .crontab("0 4 * * sun"))
```

## Documentation

You can find documentation for Jobs [here](https://docs.hummingbird.codes/2.0/documentation/jobs). The [hummingbird-examples](https://github.com/hummingbird-project/hummingbird-examples) repository has a number of examples of different uses of the library.
