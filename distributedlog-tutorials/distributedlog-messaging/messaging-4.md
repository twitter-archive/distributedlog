### Exact-Once Processing

Applications typically choose between `at-least-once` and `exactly-once` processing semantics.
`At-least-once` processing guarantees that the application will process all the log records,
however when the application resumes after failure, previously processed records may be re-processed
if they have not been acknowledged. `Exactly once` processing is a stricter guarantee where applications
must see the effect of processing each record exactly once. `Exactly once` semantics can be achieved
by maintaining reader positions together with the application state and atomically updating both the
reader position and the effects of the corresponding log records. 

This tutorial shows how to do `exact-once` processing.
