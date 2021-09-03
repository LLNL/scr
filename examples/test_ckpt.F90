      PROGRAM TEST_CKPT_F90
 
      IMPLICIT NONE
!*--TEST_CKPT_F5
 
      INCLUDE 'mpif.h'
      INCLUDE 'scrf.h'
 
      CHARACTER*1024 :: basefname = "rank_"
      CHARACTER*1024 :: fname , file_suffix
      CHARACTER(LEN=scr_max_filename) :: fname_scr
      CHARACTER(LEN=scr_max_filename) :: ckptname
      INTEGER(KIND=4) :: flag
      INTEGER(KIND=4) :: outflags
      INTEGER(KIND=4) :: valid
 
      INTEGER , PARAMETER :: NI = 20 , NJ = 30 , NK = 45
      INTEGER :: loop_count = 5
 
      INTEGER(KIND=8) , DIMENSION(NI,NJ,NK) :: w1 , r1
 
      INTEGER :: ierr , errors , all_errors , nprocs , mynod , ios
      INTEGER :: i , j , k , loop
 
      INTEGER :: writeunit , readunit
      INTEGER(KIND=8) :: nodeoff
 
!      integer (kind = 8) :: total_bytes_transferred
      REAL(KIND=8) :: total_bytes_transferred
      REAL(KIND=4) overall_transfer_rate
      REAL(KIND=8) time0 , time1 , iter_time0 , iter_time1
 
      CALL MPI_INIT(ierr)
      CALL MPI_COMM_SIZE(mpi_comm_world,nprocs,ierr)
      CALL MPI_COMM_RANK(mpi_comm_world,mynod,ierr)
 
      CALL SCR_INIT(ierr)
 
      nodeoff = 2**21
 
!     test restart interface
      CALL SCR_HAVE_RESTART(flag,ckptname,ierr)
      IF ( flag==1 ) THEN
         CALL SCR_START_RESTART(ckptname,ierr)
 
         readunit = mynod
         WRITE (file_suffix,'(i5.5)') readunit
         fname = TRIM(ckptname)//"/"//TRIM(basefname)//TRIM(file_suffix)&
               & //".ckpt"
         CALL SCR_ROUTE_FILE(fname,fname_scr,ierr)
 
         r1 = 0
         valid = 1
         OPEN (UNIT=readunit,FILE=fname_scr,FORM='unformatted',         &
              &ACTION='read')
         READ (readunit,IOSTAT=ios) r1
         CLOSE (readunit)
 
         CALL SCR_COMPLETE_RESTART(valid,ierr)
      ENDIF
 
      IF ( mynod==0 ) time0 = MPI_WTIME(ierr)
 
!     execute work loop and checkpoint
      DO loop = 1 , loop_count
 
         IF ( mynod==0 ) iter_time0 = MPI_WTIME(ierr)
 
         forall(i=1:NI,j=1:NJ,k=1:NK)w1(i,j,k) = nodeoff*mynod + i +    &
          & NI*(j-1+NJ*(k-1))
 
!       test checkpoint interface
         CALL SCR_START_CHECKPOINT(ierr)
 
         WRITE (file_suffix,'(i5.5)') loop
         ckptname = "ckpt_"//TRIM(file_suffix)
 
         writeunit = mynod
         WRITE (file_suffix,'(i5.5)') writeunit
         fname = TRIM(ckptname)//"/"//TRIM(basefname)//TRIM(file_suffix)&
               & //".ckpt"
         CALL SCR_ROUTE_FILE(fname,fname_scr,ierr)
 
         valid = 1
         OPEN (UNIT=writeunit,FILE=fname_scr,FORM='unformatted',        &
              &ACTION='write')
         WRITE (writeunit,IOSTAT=ios) w1
         CLOSE (writeunit)
 
         CALL SCR_COMPLETE_CHECKPOINT(valid,ierr)
 
!       test output interface
         WRITE (file_suffix,'(i5.5)') loop
         ckptname = "output_"//TRIM(file_suffix)
         outflags = scr_flag_checkpoint + scr_flag_output
         CALL SCR_START_OUTPUT(ckptname,outflags,ierr)
 
         writeunit = mynod
         WRITE (file_suffix,'(i5.5)') writeunit
         fname = TRIM(ckptname)//"/"//TRIM(basefname)//TRIM(file_suffix)&
               & //".ckpt"
         CALL SCR_ROUTE_FILE(fname,fname_scr,ierr)
 
         valid = 1
         OPEN (UNIT=writeunit,FILE=fname_scr,FORM='unformatted',        &
              &ACTION='write')
         WRITE (writeunit,IOSTAT=ios) w1
         CLOSE (writeunit)
 
         CALL SCR_COMPLETE_OUTPUT(valid,ierr)
 
         CALL MPI_BARRIER(mpi_comm_world,ierr)
 
         errors = 0
         CALL MPI_ALLREDUCE(errors,all_errors,1,mpi_integer,mpi_sum,    &
                          & mpi_comm_world,ierr)
         IF ( all_errors>0 ) CALL EXIT(1)
 
         IF ( mynod==0 ) THEN
            iter_time1 = MPI_WTIME(ierr)
            WRITE (6,99001) loop , iter_time1 - iter_time0
99001       FORMAT ("Iteration = ",i6,", Run Time = ",f10.4," sec.")
#ifdef __ibmxl__
            CALL FLUSH_(6)
#else
            CALL FLUSH(6)
#endif
         ENDIF
 
      ENDDO  ! loop
 
      IF ( mynod.EQ.0 ) THEN
         time1 = MPI_WTIME(ierr)
         total_bytes_transferred = REAL(2*6*loop_count)
         total_bytes_transferred = total_bytes_transferred*REAL         &
                                 & (NI*NJ*NK)
         total_bytes_transferred = total_bytes_transferred*REAL(nprocs)
 
         WRITE (6,99002) total_bytes_transferred/1.0E+06
99002    FORMAT ("Total bytes transferred = ",1pe12.3," 10e+06 Bytes")
 
         WRITE (6,99003) time1 - time0
99003    FORMAT ("Total time =              ",1pe12.3," sec")
 
         overall_transfer_rate = REAL(total_bytes_transferred)          &
                               & /((time1-time0)*1.0E+06)
         WRITE (6,99004) overall_transfer_rate
99004    FORMAT ("Overall Transfer Rate =   ",1pe12.3,                  &
                &" 10e+06 Bytes/sec")
         WRITE (6,99005)
99005    FORMAT ("all done")
      ENDIF
 
      CALL SCR_FINALIZE(ierr)
 
      CALL MPI_FINALIZE(ierr)
 
      END
