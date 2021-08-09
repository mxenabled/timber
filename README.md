Timber!
=======

Timber is an experimental utility to parse postgres logs from stdin or journald.

It currently parses slow query logs and sends a json payload to LOCAL1.

```
Usage of ./timber:
  -logger-source-type string
        supports stdin for piped input and journald (default "stdin")
  -tcp-out-url string
        if set, will set up a log sink to given tcp destination
  -version
        show the version and exit
```

License MIT.
