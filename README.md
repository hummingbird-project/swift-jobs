<p align="center">
<picture>
  <source media="(prefers-color-scheme: dark)" srcset="https://github.com/hummingbird-project/hummingbird/assets/9382567/48de534f-8301-44bd-b117-dfb614909efd">
  <img src="https://github.com/hummingbird-project/hummingbird/assets/9382567/e371ead8-7ca1-43e3-8077-61d8b5eab879">
</picture>
</p>  
<p align="center">
<a href="https://swift.org">
  <img src="https://img.shields.io/badge/swift-5.9-brightgreen.svg"/>
</a>
<a href="https://github.com/hummingbird-project/swift-jobs/actions?query=workflow%3ACI">
  <img src="https://github.com/hummingbird-project/swift-jobs/actions/workflows/ci.yml/badge.svg?branch=main"/>
</a>
<a href="https://discord.gg/7ME3nZ7mP2">
  <img src="https://img.shields.io/badge/chat-discord-brightgreen.svg"/>
</a>
</p>

# Jobs

Offload work from your server to other servers.


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

## Documentation

You can find documentation for Jobs [here](https://docs.hummingbird.codes/2.0/documentation/jobs). The [hummingbird-examples](https://github.com/hummingbird-project/hummingbird-examples) repository has a number of examples of different uses of the library.
