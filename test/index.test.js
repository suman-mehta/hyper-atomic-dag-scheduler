import test from 'node:test';
import assert from 'node:assert/strict';
import { DagScheduler } from '../src/index.js';

/**
 * Simple async helper that resolves after a tick.
 */
const tick = () => new Promise(r => setImmediate(r));

test('DagScheduler executes tasks respecting dependencies', async (t) => {
  const execOrder = [];
  const scheduler = new DagScheduler({ workerCount: 2 });

  scheduler.addTask('A', async () => { await tick(); execOrder.push('A'); }, []);
  scheduler.addTask('B', async () => { await tick(); execOrder.push('B'); }, ['A']);
  scheduler.addTask('C', async () => { await tick(); execOrder.push('C'); }, ['A']);
  scheduler.addTask('D', async () => { await tick(); execOrder.push('D'); }, ['B', 'C']);

  await scheduler.start();

  // Verify that all tasks ran
  assert.deepEqual(execOrder.sort(), ['A', 'B', 'C', 'D'].sort());
  // Verify ordering constraints
  const idx = (id) => execOrder.indexOf(id);
  assert.ok(idx('A') < idx('B'), 'A before B');
  assert.ok(idx('A') < idx('C'), 'A before C');
  assert.ok(idx('B') < idx('D'), 'B before D');
  assert.ok(idx('C') < idx('D'), 'C before D');
});
