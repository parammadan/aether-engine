# Deployment cost rules (hard rules, not suggestions)

| What | Value |
|---|---|
| Instances | 4 × t4g.small (Graviton, 2 vCPU / 2 GiB) |
| Hourly cost | ~$0.0168/instance ≈ **$0.07/hour total** (+ ~4×8GiB gp3 EBS ≈ $0.01/day) |
| Region | us-east-2 (account default) |
| **Max session** | **2 hours — never leave the cluster running unattended** |
| Teardown | `./teardown.sh` after EVERY run; it verifies zero tagged resources remain |

Every resource this deployment creates carries the tag `aether=1`. Teardown destroys by
tag, so nothing this repo provisions can be orphaned invisibly. If in doubt:

    aws ec2 describe-instances --filters Name=tag:aether,Values=1 \
      Name=instance-state-name,Values=running,pending,stopping,stopped

should return nothing after teardown.
