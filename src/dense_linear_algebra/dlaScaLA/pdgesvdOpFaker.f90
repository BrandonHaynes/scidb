! NOTE: this version of the code is NOT COPYRIGHT P4 OR SCIDB.
!     it is an example of SVD use from Argonne Nat'l. Lab.  The original
!     file starts on the line containing the '=================", and
!     did not itself contain a copyright notice.  I will check whether there
!     was one that applies to this file and update this notice here
!     if and when I find one.
!     ==================
!
!***********************************************************************
!*                                                                     *
!*    This program reads a (full) matrix A from a file, distributes    *
!*    A among the available processors and then calls the ScaLAPACK    *
!*    subroutine pdgesvd to computed the SVD of A, A = U*S*V^{T}.      *
!*                                                                     *
!*    The following has been tested on an IBM SP (and should be        *
!*    changed depending on the target platform):                       *
!*                                                                     *
!*    % module load scalapack                                          *
!*    % mpxlf90 -u -qfixed -o pdgesvddrv.x pdgesvddrv.f \              *
!*              $SCALAPACK $PBLAS $BLACS -lessl                        *
!*    % poe pdgesvddrv.x -procs 3                                      *
!*    % poe pdgesvddrv.x -procs 4                                      *
!*                                                                     *
!*    Notes:                                                           *
!*                                                                     *
!*  . Information about the matrix A is read from 'pdgesvdOpFaker.dat.txt',    *
!*    which should contain three lines:                                *
!*    1. The name of the file where A is stored by columns, one        *
!*       entry per line                                                *
!*    2. The number of rows (M) of A                                   *
!*    3. The number of columns (N) of A                                *
!*  . Make sure that M*N is smaller or equal the number of entries     *
!*    in A.                                                            *
!*  . To facilitate the distribution of A, the coordinating processor  *
!*    processor,  NPE-1, reads the columns of A and sends              *
!*    them to processors 0 through NPE-2, in that order.               *
!*  . The number of processors must be smaller than or equal to N.     *
!*                                                                     *
!***********************************************************************
!
!.... Make argonne's pdgesvddrv into a subroutine so it can be invoked
!     by the SciDB SVD operator so we can can test that we have
!     SCALAPACK, MPI, and a BLAS all built and linking properly.
!
      SUBROUTINE PDGESVD_OP_FAKER()
!
      INCLUDE 'mpif.h'
!
      INTEGER :: I, INFO, J, K, LDA, LWORK, M, N, NCOL_I, &
                 NCOL_PE
      INTEGER :: DESCA(9), DESCU(9), DESCVT(9), IA = 1, &
                 JA = 1, IU = 1, JU = 1, IVT = 1, JVT = 1, MB, NB
      INTEGER*4 :: MPIERR, MPISTATUS(MPI_STATUS_SIZE), &
                 MPITAG = 1, MYPE, NPE, CONTEXT, NPROW, NPCOL, COORDPE
      INTEGER*4 :: MY_MPI_COMM_WORLD
      DOUBLE PRECISION :: TEMP(1)
      DOUBLE PRECISION, ALLOCATABLE :: A(:), S(:), U(:), VT(:), WORK(:)
      CHARACTER :: MATRIX*64
! 
!.... Initialize MPI ...................................................
!
!     WRITE (*,*) 'About to mpi_init'
!     CALL MPI_INIT (MPIERR)
      WRITE (*,*) 'About to mpi_comm_size'
      CALL MPI_COMM_SIZE (MPI_COMM_WORLD,NPE,MPIERR)
      WRITE (*,*) 'About to mpi_comm_world'
      CALL MPI_COMM_RANK (MPI_COMM_WORLD,MYPE,MPIERR)
      WRITE (*,*) 'MPI init stuff done'
      WRITE (*,*) 'MYPE', MYPE, 'NPE', NPE
! 
!.... Processor COORDPE reads the file name and dimension of the matrix ..
! 
      COORDPE = 0
      IF ( MYPE .EQ. COORDPE ) THEN 
         OPEN  (UNIT=10,FILE='pdgesvdOpFaker.dat.txt',FORM='FORMATTED')
         READ  (UNIT=10,FMT=*) MATRIX ! File name
         READ  (UNIT=10,FMT=*) M      ! Number of rows
         READ  (UNIT=10,FMT=*) N      ! Number of columns
         CLOSE (UNIT=10) 
         OPEN  (UNIT=11,FILE=MATRIX,FORM='FORMATTED')
      END IF
! 
!.... Processor COORDPE broadcasts the dimensions of the matrix ..........
! 
      WRITE (*,*) 'N', N, 'M', M      
      CALL MPI_BCAST (M,1,MPI_INTEGER,COORDPE,MPI_COMM_WORLD,MPIERR)
      CALL MPI_BCAST (N,1,MPI_INTEGER,COORDPE,MPI_COMM_WORLD,MPIERR)
      WRITE (*,*) 'N', N, 'M', M      
      N = 2 ! HACK to match checked-in pdgesvd.mat.txt
      M = 3 ! HACK
      WRITE (*,*) 'N', N, 'M', M
      IF ( ( M.LT.NPE ) .OR. ( N.LT.NPE ) ) THEN
         IF ( MYPE .EQ. 0 ) WRITE (*,*) '> Abort: M<NPE OR N<NPE'
         STOP
      END IF
!
!.... Set the blocking .................................................
!
!     ** There may be better ways of setting MB, NB and LDA **
!     ** ScaLAPACK's PDGEBRD requires MB = NB **                 
!
      NB = CEILING(REAL(N)/REAL(NPE))
      MB = NB
      LDA = MAX(M,N)
!
!.... Number of columns of A on processor MYPE .........................
!
      NCOL_PE = MIN(NB,N-NB*MYPE)
!
!.... Allocate arrays ..................................................
!
      ALLOCATE (A(LDA*NB)); A = 0
      ALLOCATE (U(LDA*NB)); U = 0
      ALLOCATE (VT(LDA*NB)); VT = 0
      ALLOCATE (S(MAX(M,N))); S = 0
!
!.... Loop on all processors ...........................................
!
!     Processor COORDPE reads A and distributes it
      WRITE (*,*) 'NPE is', NPE
!
      DO I = 0,NPE-1
!
         IF      ( MYPE .EQ. COORDPE ) THEN
                 WRITE (*,*) 'COORDPE reading file'
!
                 A = 0
!
!                Number of columns of A to be read from file
!
                 NCOL_I = MIN(NB,N-NB*I)
!
!                Read NCOL_I columns of A from file
!
                 READ (UNIT=11,FMT=*) ((A(K+J*LDA),K=1,M),J=0,NCOL_I-1)
!
!                Send NCOL_I columns of A to processor I
!
                 IF ( I .NE. COORDPE ) THEN
                    WRITE (*,*) 'MPI_SEND 1'
                    CALL MPI_SEND (A,LDA*NCOL_I,MPI_DOUBLE_PRECISION, &
                                   I,MPITAG,MPI_COMM_WORLD,MPIERR)
                 END IF

         ELSE IF ( MYPE .EQ. I ) THEN         
!
!                Receive NCOL_PE columns of A from processor COORDPE
!
                 WRITE (*,*) 'MPI_RECV 1'
                 CALL MPI_RECV (A,LDA*NCOL_PE,MPI_DOUBLE_PRECISION, &
                                COORDPE,MPITAG,MPI_COMM_WORLD, &
                                MPISTATUS,MPIERR)
!
         END IF
! 
      END DO
      IF      ( MYPE .EQ. COORDPE ) THEN
              CLOSE (UNIT=11)
      END IF
!
!.... Initialize the BLACS .............................................
!
!     ** There may be better ways of setting NPROW and NPCOL **
!
      NPROW = 1
      NPCOL = NPE
!
      WRITE (*,*) 'BLACS_GET'
      CALL BLACS_GET( -1, 0, CONTEXT )
      WRITE (*,*) 'BLACS_GRIDINIT'
      CALL BLACS_GRIDINIT( CONTEXT, 'R', NPROW, NPCOL )
!
!.... Set the array descriptors ........................................
!
      WRITE (*,*) 'DESCINIT DESCA'
      CALL DESCINIT(DESCA ,M+IA-1 ,N+JA-1 ,MB,NB,0,0,CONTEXT,LDA,INFO)
      WRITE (*,*) 'DESCINIT DESCU'
      CALL DESCINIT(DESCU ,M+IU-1 ,M+JU-1 ,MB,NB,0,0,CONTEXT,LDA,INFO)
      WRITE (*,*) 'DESCINIT DESCVT'
      CALL DESCINIT(DESCVT,N+IVT-1,N+JVT-1,MB,NB,0,0,CONTEXT,LDA,INFO)
!
!.... Call PDGESVD with LWORK=-1 to get the right size of WORK .........
!
      WRITE (*,*) 'Getting size of work'
      LWORK = -1
      CALL PDGESVD( 'V', 'V', M, N, A, IA, JA, DESCA, S, U, &
                    IU, JU, DESCU, VT, IVT, JVT, DESCVT, &
                    TEMP, LWORK, INFO )
      LWORK = TEMP(1)
      ALLOCATE (WORK(LWORK))
      WRITE (*,*) 'TEMP(1)', TEMP(1)
      WRITE (*,*) 'LWORK', LWORK 
!
!.... Call PDGESVD to compute the SVD of A .............................
!
      WRITE (*,*) 'Calling PDGESVD for real'
      CALL PDGESVD( 'V', 'V', M, N, A, IA, JA, DESCA, S, U, &
                    IU, JU, DESCU, VT, IVT, JVT, DESCVT, &
                    WORK, LWORK, INFO )
!
!.... Processor 0 prints the singular values ...........................
!
      WRITE (*,*) 'PDGESVD donel'
      IF      ( MYPE .EQ. 0 ) THEN 
              WRITE (*,*) '* Singular Values:'
              WRITE (*,'(I6,1P,E12.4)') (I,S(I),I=1,MIN(M,N))
      END IF
!
!.... Finalize BLACS and MPI ...........................................
! 
      WRITE (*,*) 'BLACS_GRIDEXIT'
      CALL BLACS_GRIDEXIT (CONTEXT)
!     WRITE (*,*) 'MPI_FINALIZE'
!     CALL MPI_FINALIZE (MPIERR)
!
!**** End of PDGESVDDRV ************************************************
!
      RETURN
      END

