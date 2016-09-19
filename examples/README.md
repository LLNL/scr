# SCR Examples

This directory includes a number of sample programs for working with the SCR API.

## Building the Tests

The example programs are not automatically built after SCR installation.
Instead, they must be built separately after running `make install` for the SCR distribution.
These examples are installed to `${SCR_INSTALL}/share/scr/examples/` and can be built using `make`.

## Program Descriptions

### Test Checkpoint C/Fortran

These programs perform a lightweight test of the SCR API.
Each process creates a single checkpoint file saying "hi".

### Test API (Multiple)

This sample program emulates an application which performs periodic checkpointing.
Each process creates one (or multiple) checkpoint files during each checkpoint phase.
Sample usage for the `test_api` program can be found in the scripts within the `testing/` directory.

### Test Interpose (Multiple)

*These tests are deprecated.*

These programs do not use the SCR API.
