SCR State
=========

SCR's internal state is managed in :scr:variable:`scr_state`.

.. scr:variable:: scr_state
   SCR_STATE_UNINIT
   SCR_STATE_IDLE
   SCR_STATE_CHECKPOINT
   SCR_STATE_OUTPUT
   SCR_STATE_RESTART

A generic SCR API function should do the following checks before running:

0. Check that any input parameters (flag pointers) have been allocated.
1. Check if SCR is enabled.
   If not, return with default (no-op) status and actions.
2. Check if SCR is initialized.
   If not, abort with an appropriate message.
3. Check and handle the SCR state transition.
   If the starting state is incorrect, abort using ``scr_state_transition_abort``.
   Don't forget to change the current state after this check.
