# Fault Tolerance Demonstration

## Topic Configuration

The `sensor-events` topic is configured with:
- **Partitions**: 3 (distributes load across brokers)
- **Replication Factor**: 3 (each partition has 3 copies)
- **min.insync.replicas**: 2 (at least 2 replicas must acknowledge writes)

## Leader Distribution (Before Failure)

Run the following command to inspect the topic:

```bash
docker exec kafka1 kafka-topics --bootstrap-server kafka1:29091 --describe --topic sensor-events
```

Expected output pattern:

```
Topic: sensor-events  PartitionCount: 3  ReplicationFactor: 3
  Partition: 0  Leader: 1  Replicas: 1,2,3  Isr: 1,2,3
  Partition: 1  Leader: 2  Replicas: 2,3,1  Isr: 2,3,1
  Partition: 2  Leader: 3  Replicas: 3,1,2  Isr: 3,1,2
```

Each partition has a leader on a different broker, and all three replicas are in-sync (ISR).

## Fault Injection: Stop kafka2

```bash
docker stop kafka2
```

## Leader Re-Election (After Failure)

After stopping kafka2, re-run the describe command:

```bash
docker exec kafka1 kafka-topics --bootstrap-server kafka1:29091 --describe --topic sensor-events
```

Expected output pattern (Partition 1 leader re-elected to broker 1 or 3):

```
Topic: sensor-events  PartitionCount: 3  ReplicationFactor: 3
  Partition: 0  Leader: 1  Replicas: 1,2,3  Isr: 1,3
  Partition: 1  Leader: 3  Replicas: 2,3,1  Isr: 3,1
  Partition: 2  Leader: 3  Replicas: 3,1,2  Isr: 3,1
```

Key observations:
1. **Leader re-election**: Partition 1's leader changed from broker 2 to another broker, showing automatic failover.
2. **ISR shrinks**: The in-sync replica count drops from 3 to 2 for affected partitions.
3. **Producers unaffected**: With `min.insync.replicas=2`, writes continue to succeed as long as 2 replicas remain.
4. **No data loss**: All committed data is preserved on the remaining replicas.

## Recovery: Restart kafka2

```bash
docker start kafka2
```

After a brief catch-up period, kafka2 rejoins the ISR and the cluster returns to full health with all 3 replicas in sync.

## Conclusion

The cluster tolerates a single broker failure without data loss or producer blocking, satisfying the reliability requirements of the platform.
