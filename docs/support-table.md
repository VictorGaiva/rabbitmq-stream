# Support Table

| Command                 | From            | Key    | Expects response? | Supported? |
|-------------------------|-----------------|--------|-------------------|------------|
| declarepublisher        | Client          | 0x0001 | Yes               | ✔️          |
| publish                 | Client          | 0x0002 | No                | ✔️          |
| publishconfirm          | Server          | 0x0003 | No                | ✔️          |
| publisherror            | Server          | 0x0004 | No                | ✔️          |
| querypublishersequence  | Client          | 0x0005 | Yes               | ✔️          |
| deletepublisher         | Client          | 0x0006 | Yes               | ✔️          |
| subscribe               | Client          | 0x0007 | Yes               | ✔️          |
| deliver                 | Server          | 0x0008 | No                | ✔️          |
| credit                  | Client          | 0x0009 | No                | ✔️          |
| storeoffset             | Client          | 0x000a | No                | ✔️          |
| queryoffset             | Client          | 0x000b | Yes               | ✔️          |
| unsubscribe             | Client          | 0x000c | Yes               | ✔️          |
| create                  | Client          | 0x000d | Yes               | ✔️          |
| delete                  | Client          | 0x000e | Yes               | ✔️          |
| metadata                | Client          | 0x000f | Yes               | ✔️          |
| metadataupdate          | Server          | 0x0010 | No                | ✔️          |
| peerproperties          | Client          | 0x0011 | Yes               | ✔️          |
| saslhandshake           | Client          | 0x0012 | Yes               | ✔️          |
| saslauthenticate        | Client          | 0x0013 | Yes               | ✔️          |
| tune                    | Server          | 0x0014 | Yes               | ✔️          |
| open                    | Client          | 0x0015 | Yes               | ✔️          |
| close                   | Client & Server | 0x0016 | Yes               | ✔️          |
| heartbeat               | Client & Server | 0x0017 | No                | ✔️          |
| route                   | Client          | 0x0018 | Yes               | ❌          |
| partitions              | Client          | 0x0019 | Yes               | ❌          |
| consumerupdate          | Server          | 0x001a | Yes               | ❌          |
| exchangecommandversions | Client          | 0x001b | Yes               | ❌          |
| streamstats             | Client          | 0x001c | Yes               | ❌          |
| createsuperstream       | Client          | 0x001d | Yes               | ❌          |
| deletesuperstream       | Client          | 0x001e | Yes               | ❌          |
