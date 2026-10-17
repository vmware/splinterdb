Resident async-mask comparison; percent throughput change, paired block estimate and 95% interval.

| API | Workers | Access | vs main | vs original |
|---|---:|---|---:|---:|
| blocking | 1 | independent | +0.41% [-2.90, +3.84] | +0.56% [-0.19, +1.31] |
| blocking | 1 | dependent | +0.19% [-2.29, +2.73] | -0.15% [-3.91, +3.75] |
| blocking | 8 | independent | +0.83% [-2.36, +4.12] | +1.65% [-0.31, +3.65] |
| blocking | 8 | dependent | -1.08% [-3.31, +1.20] | -0.18% [-0.99, +0.63] |
| async | 1 | independent | +1.67% [-2.26, +5.75] | +2.03% [+0.59, +3.50] |
| async | 1 | dependent | +0.60% [-3.79, +5.19] | +0.37% [-2.71, +3.54] |
| async | 8 | independent | +0.58% [-0.87, +2.05] | +1.94% [+0.65, +3.24] |
| async | 8 | dependent | +0.50% [+0.01, +0.99] | +1.42% [+0.98, +1.87] |

144 measured runs, 8 excluded preconditions, no measured samples removed. Separate scenarios and harnesses are not pooled.
Counters: single-worker sync −1 instruction/op versus original (+3 versus main); async −10 versus original (−2 versus main).
Worst mean loss is 1.08% versus main; two main intervals extend below −3%. All processes record thermal-counter increments outside a precisely located phase.
