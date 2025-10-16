```
DS/
├── src/
│   ├── __init__.py              
│   ├── common/
│   │   ├── grpc/
│   │   │   ├── auto_generated   # put the automatically generated code by compiler here
│   │   │   ├── protos
│   │   │   │   ├── messages     # put messages here
│   │   │   │   ├── services     # put services here
│   │   ├── __init__.py
│   │   └── utils.py             # common functions
│   ├── afs/                     # main system
│   │   ├── __init__.py
│   │   ├── client/
│   │   └── server/
│   └── app/                     # task 2 prime number app logic
├── config/                       # put setting files here (related to server blablabla)
├── tests/                        # put tests scripts (here)
├── scripts/                      # put startup scripts here
├── docker/
├── docs/                         # design and function docs here
│   └── design.md
├── requirements.txt              # Python dependencies
├── setup.py                      # pakaging setting (optional)
├── .env                          # environment variable setting (sensitive settings, pls put this file to gitignore and share only with each other) 
├── .gitignore                    # Git ignore
└── README.md                     # project notification
```
