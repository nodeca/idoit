1.2.1 / 2017-03-29
------------------

- Fix empty result store in chains & groups.


1.2.0 / 2017-03-16
------------------

- Add API method to force task restart, #2.
- Implemented active chunks tracking for iterators.


1.1.1 / 2017-03-03
------------------

- Quick-fix to suppress events from iterator's children when
  parent was canceled.


1.1.0 / 2017-02-27
------------------

- Simplify extending group/chain task via `.init()` method override.


1.0.1 / 2017-02-07
------------------

- Fix `task:end` event emit on task cancel (for nested tasks).


1.0.0 / 2016-11-04
------------------

- First release.
