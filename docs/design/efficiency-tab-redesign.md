# Efficiency tab redesign

## Goal

Keep the detail, but give the page a clear reading order:

**How we’re doing → what changed → which projects need attention → individual jobs.**

Within five seconds, a user should be able to tell whether targets are being met, whether performance is improving, and which project to inspect first.

This document is a design proposal, not a description of an implemented redesign.

## Current problem

The deployed Efficiency tab presents methodology, targets, reliability, filters, totals, and three detailed tables at similar visual weight. Users must inspect the numbers and calculate the overall story mentally.

The detail is valuable. The page needs a stronger overview and a clear path into that detail.

## Design principles

| Reference | Principle | Application |
|---|---|---|
| Steve Krug, *Don’t Make Me Think* | Make the page easy to scan. | Answer “Are we hitting targets?” immediately, with visible status and comparison. |
| Don Norman, *The Design of Everyday Things* | Provide a clear conceptual model. | Distinguish CPU efficiency, memory sizing, and reliability. Each measures something different. |
| Adam Wathan and Steve Schoger, *Refactoring UI* | Establish visual hierarchy. | Give headline results prominence; use quieter styling for denominators and supporting detail. |
| Jenifer Tidwell et al., *Designing Interfaces* | Organize overview and detail. | Start with the portfolio, select a project, then inspect workflows and jobs. |
| Jakob Nielsen, usability heuristics | Show status and favor recognition. | Display targets beside results, date ranges beside comparisons, and freshness beside the report. |

These are applications of the principles to this dashboard, rather than quotations from the references.

## 1. A compact answer at the top

Default to **This week**, with obvious period controls:

**Today · This week · Last week · Last 30 days · Custom**

Keep project and backend filters nearby. Put workflow and advanced controls behind **More filters**.

Show three prominent cards:

| CPU efficiency | Memory sizing | Resource failures |
|---|---|---|
| Large percentage | Median peak / requested RAM | OOM + timeout rate |
| Target ≥80% | Target band 60–80% | Target <1% |
| Change vs comparison period | Below / within / above band | Counts and evidence status |

These targets reflect the configured policy at the time of this proposal. The implementation should read the policy rather than hard-code these values.

Each card should include:

- A prominent result.
- A short status, such as **Below target**, **Within target**, or **Limited evidence**.
- A small visual showing the result relative to its target.
- Compact measurement coverage and relevant counts.

Do not combine these into a single efficiency score. High memory utilization is not always better, and strong CPU utilization can coexist with excessive failures.

## 2. Trends that answer “Are we improving?”

Use three aligned charts with a shared time axis:

- **CPU:** daily weighted efficiency, with the configured target line.
- **Memory:** daily median peak/request ratio, with the configured target band shaded.
- **Failures:** daily resource failure rate, with counts available when selected.

A restrained bar chart underneath can show allocated CPU-hours, distinguishing busy days from lightly sampled ones. Missing measurements should create gaps, not apparent zeros.

For **This week**, compare against the same elapsed portion of last week. Label the comparison explicitly. Keep the complete **Last week** view one click away.

Show the reporting timezone and concrete date ranges. The current report filters completion dates in UTC; any change to that convention must be explicit and consistent across filters, charts, and comparisons.

## 3. Project comparison that directs attention

Use a compact table with graphics inside the rows:

| Project | CPU vs target | Memory vs band | Failures | CPU-hours |
|---|---|---|---|---|
| Project name ↗ | Bar + percentage + change | Dot against target band | Rate + count | Workload size |

This preserves numerical precision while making projects visually comparable.

- Use consistent scales across project rows.
- Pair status colors with words; do not rely on color alone.
- Provide a **Needs attention** sort that considers both target shortfall and workload size.
- Make each project an obvious entry point into its details.

A large project slightly below target can deserve more attention than a tiny project with an extreme percentage. Define the attention ordering transparently during implementation rather than presenting an unexplained score.

## 4. Details when a project is selected

Selecting a project should expose its workflows and job table while preserving the period and filters. Selecting a job should open its measurements, requests, failure reason, and run link.

Move the following into this deeper view:

- Mean, median, p95, and maximum memory statistics.
- Measurement provenance and accounting explanations.
- Per-job resource requests and measurements.
- Detailed failure and coverage information.

Keep a clear route back to the portfolio overview without losing the selected reporting period.

This is progressive disclosure: retain the capability while reducing what users must process initially.

## Measurement and trust requirements

- Label the report **completed-job efficiency**. The current report selects jobs by completion time; it does not measure instantaneous utilization of running jobs.
- Compute overall CPU efficiency from summed consumed CPU time divided by summed allocated CPU time over the same measured jobs. Do not average job percentages.
- Preserve the current distinction between CPU accounting, which includes measured failed jobs, and memory sizing distributions, which use successful jobs with comparable measurements.
- Describe memory measurements as job peaks, not time-averaged memory usage.
- Keep coverage visible in compact form. Missing measurements are not zero.
- Preserve unknown outcomes and limited-evidence states. Zero observed failures with little evidence must not imply established reliability.
- Apply the configured minimum-attempts rule when displaying reliability assessments; the policy currently specifies 100 attempts.
- Keep targets and their interpretation next to the result they qualify.
- Show data freshness so users can distinguish delayed accounting from current results.

## Visual direction

Use a restrained palette, generous spacing between sections, and consistent alignment. Give the three headline results the strongest emphasis, trends the next level, and project details a compact, quieter treatment.

Reserve status colors for meaningful target assessments. Supporting labels, explanations, and denominators should remain readable without competing with the primary result.

## Acceptance criteria

- Users can identify target status, direction of change, and the first project to inspect within five seconds.
- Today, this week, and last week are accessible without entering dates manually.
- Every comparison names its reference period.
- The default view shows the portfolio summary, trends, and project comparison before job-level detail.
- Project and job drill-down preserve the reporting context.
- Missing data and limited evidence are visibly distinct from good performance.
- Existing detailed measurements remain accessible.

## References

- [Steve Krug — usability and Don’t Make Me Think](https://sensible.com/)
- [Don Norman — Design as Communication](https://jnd.org/design-as-communication/)
- [Refactoring UI](https://refactoringui.com/)
- Jenifer Tidwell et al., *Designing Interfaces*.
- [Nielsen’s 10 Usability Heuristics](https://www.nngroup.com/articles/ten-usability-heuristics/)
- [Nielsen Norman Group — Progressive Disclosure](https://www.nngroup.com/articles/progressive-disclosure/)
