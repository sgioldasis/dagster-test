## DLT Freshness UI Guide - Manual Mode

### 🎯 **Problem Solved**

Your DLT assets will now show "Overdue" in the Dagster UI lineage **even without automatic runs** because:

1. ✅ **Freshness Policy**: Keeps the asset registered in the freshness system
2. ✅ **Materialization Events**: Assets have history from previous runs
3. ✅ **Manual Control**: No automatic runs, but UI still shows status

### 🔧 **Current Configuration**

```python
# Assets have:
# - Freshness Policy: ✅ (1-minute tolerance, checks every minute)
# - Automation Condition: ❌ (None - no automatic runs)
# - Manual Schedule: ✅ (Run every 2 minutes for testing)
```

### 📊 **Expected Behavior**

1. **Freshness Evaluation**: Every minute, Dagster checks if assets are fresh
2. **Overdue Detection**: After 2 minutes without materialization, assets show "Overdue" 
3. **No Auto-Runs**: Assets don't automatically run when overdue
4. **Manual Control**: You can run assets manually via UI or schedule

### 🚀 **How to Use**

1. **Start Dagster**: `dg dev --workspace workspace.yaml`
2. **Wait for Overdue**: After 2 minutes of inactivity, check UI
3. **Manual Runs**: Use the schedule or UI to trigger runs manually
4. **Monitor Status**: "Overdue" indicator appears in lineage view

### 🔍 **UI Indicators to Look For**

- **Green Check**: Asset is fresh (materialized within last minute)
- **Yellow Clock**: Asset is stale (overdue but within tolerance)
- **Red Exclamation**: Asset is overdue (past tolerance)
- **Blue Play**: Manual run available (no automation)

### 📝 **Testing Steps**

1. Stop any current runs
2. Wait 2+ minutes without running assets
3. Check Dagster UI → Asset Lineage
4. Look for "Overdue" status on DLT assets
5. Verify no automatic runs are triggered

This setup gives you the visual feedback of freshness status without the automatic execution, perfect for monitoring and manual control.