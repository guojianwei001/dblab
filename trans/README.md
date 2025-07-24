gcc -o sql_server_sim sql_server_sim.c -lpthread
./sql_server_sim


Output:
====================================================
     STARTING SIMULATION FOR: READ COMMITTED
====================================================
[Reader Thread 140470405281472, Level: READ_COMMITTED] Reading balance of account 0 for the 1st time...
[Reader Thread 140470405281472] First read balance: 1000
[Writer Thread 140470396888768] Transferring 100 from account 0 to 1.
[Reader Thread 140470405281472] Reading balance of account 0 for the 2nd time...
[Reader Thread 140470405281472] Second read balance: 900

        !!! NON-REPEATABLE READ DETECTED on thread 140470405281472 !!!



====================================================
     STARTING SIMULATION FOR: REPEATABLE READ
====================================================
[Reader Thread 140470396888768, Level: REPEATABLE_READ] Reading balance of account 0 for the 1st time...
[Reader Thread 140470396888768] First read balance: 1000
[Writer Thread 140470405281472] Transferring 100 from account 0 to 1.
[Reader Thread 140470396888768] Reading balance of account 0 for the 2nd time...
[Reader Thread 140470396888768] Second read balance: 1000

        >>> Repeatable Read successful on thread 140470396888768 >>>
