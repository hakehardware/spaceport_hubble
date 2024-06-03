subspace_images = [
    'subspace/farmer',
    'subspace/node'
]

farm_statuses = [
    'Initializing',
    'Plotting',
    'Replotting',
    'Farming'
]

farmer_statuses = [
    'Initializing',
    'Syncing Piece Cache',
    'Farming'
]

key_events = [
    {
        'event_name': 'Connecting to node RPC url',                    
        'event_type': 'farmer',     
        'event_subtype': 'controller',      
        'event_description': 'Farmer is connecting to the Node via the RPC URL',
        'event_level': None,
        'event_datetime': None,
        'event_data': None
    },
    {
        'event_name': 'Detecting L3 Cache Groups',                     
        'event_type': 'farmer',     
        'event_subtype': 'plotter',         
        'event_description': 'Farmer is detecting L3 Cache Groups'
    },
    {
        'event_name': 'Preparing Plotting Thread Pools',               
        'event_type': 'farmer',     
        'event_subtype': 'plotter',         
        'event_description': 'Clusters will only get Plotting Pool, otherwise Plotting and Replotting pools will be created'
    },
    {
        'event_name': 'Checking Plot Cache Contents',                  
        'event_type': 'farmer',     
        'event_subtype': 'farmer',          
        'event_description': 'Farmer checking plot cache contents'
    },
    {
        'event_name': 'Finished Checking Plot Cache Contents',         
        'event_type': 'farmer',     
        'event_subtype': 'farmer',          
        'event_description': 'The Farmer has finished checking plot cache contents'
    },
    {
        'event_name': 'Benchmarking Proving Method',                   
        'event_type': 'farmer',     
        'event_subtype': 'farmer',          
        'event_description': 'The Farmer in running benchmarks on each Farm to find the fastest proving method.'
    },
    {
        'event_name': 'Subscribing to Slot Info Notifications',        
        'event_type': 'farm',       
        'event_subtype': 'farmer',          
        'event_description': 'Farm is subscribing to slot info notifications'
    },
    {
        'event_name': 'Subscribing to Reward Signing Notifications',   
        'event_type': 'farm',       
        'event_subtype': 'farmer',          
        'event_description': 'Farm is subscribing to reward signing notifications'
    },
    {
        'event_name': 'Subscribing to Archived Segments',              
        'event_type': 'farm',       
        'event_subtype': 'farmer',  
        'event_description': 'Farm is subscribing to archived segments'        
        },
    {
        'event_name': 'Found Fastest Mode',                            
        'event_type': 'farm',       
        'event_subtype': 'farmer',          
        'event_description': 'Farm found fastest proving mode'
    },
    {
        'event_name': 'Register Farm ID',                              
        'event_type': 'farm',       
        'event_subtype': 'farmer',          
        'event_description': 'Register Farm ID'
    },
    {
        'event_name': 'Register Genesis Hash',                         
        'event_type': 'farm',       
        'event_subtype': 'farmer',          
        'event_description': 'Register Genesis Hash'
    },
    {
        'event_name': 'Register Public Key',                           
        'event_type': 'farm',       
        'event_subtype': 'farmer',          
        'event_description': 'Register Public Key'
    },
    {
        'event_name': 'Register Allocated Space',                      
        'event_type': 'farm',       
        'event_subtype': 'farmer',          
        'event_description': 'Register Allocated Space'
    },
    {
        'event_name': 'Register Directory',                            
        'event_type': 'farm',       
        'event_subtype': 'farmer',          
        'event_description': 'Register Directory'
    },
    {
        'event_name': 'Collecting Plotted Pieces',                     
        'event_type': 'farmer',     
        'event_subtype': 'none',            
        'event_description': 'Farmer is collecting plotted pieces'
    },
    {
        'event_name': 'Finished Collecting Plotted Pieces',            
        'event_type': 'farmer',     
        'event_subtype': 'none',            
        'event_description': 'Farmer finished collecting plotted pieces'
    },
    {
        'event_name': 'Initializing Piece Cache',                      
        'event_type': 'farmer',     
        'event_subtype': 'controller',      
        'event_description': 'Farmer is initializing the piece cache'
    },
    {
        'event_name': 'Syncronizing Piece Cache',                      
        'event_type': 'farmer',     
        'event_subtype': 'controller',      
        'event_description': 'Farmer is syncronizing the piece cache'
    },
    {
        'event_name': 'Piece Cache Sync',                              
        'event_type': 'farmer',     
        'event_subtype': 'controller',      
        'event_description': 'Farmer is syncing the piece cache'
    },
    {
        'event_name': 'Finished Piece Cache Syncronization',           
        'event_type': 'farmer',     
        'event_subtype': 'controller',      
        'event_description': 'Farmer finished syncronizing the piece cache'
    },
    {
        'event_name': 'Plotting Sector',                               
        'event_type': 'farm',       
        'event_subtype': 'farmer',          
        'event_description': 'Farm is plotting a sector'
    },
    {
        'event_name': 'Signed Reward Hash',                            
        'event_type': 'farm',       
        'event_subtype': 'farmer',          
        'event_description': 'Farm has signed a reward hash'
    },
    {
        'event_name': 'Initial Plotting Complete',                     
        'event_type': 'farm',       
        'event_subtype': 'farmer',          
        'event_description': 'Farm has completed initial plotting'
    },
    {
        'event_name': 'Replotting Sector',                             
        'event_type': 'farm',       
        'event_subtype': 'farmer',          
        'event_description': 'Farm is replotting a sector'
    },
    {
        'event_name': 'Replotting Complete',                           
        'event_type': 'farm',       
        'event_subtype': 'farmer',          
        'event_description': 'Farm has completed replotting'
    },
    {
        'event_name': 'Failed to Send Solution',                       
        'event_type': 'farm',       
        'event_subtype': 'farmer',          
        'event_description': 'Farm failed to send a solution and missed a reward'
    },
    {
        'event_name': 'Plot Sector Request',                           
        'event_type': 'farmer',     
        'event_subtype': 'plotter',         
        'event_description': 'Plotter has received a plot request'
    },
    {
        'event_name': 'Finished Plotting Sector',                      
        'event_type': 'farmer',     
        'event_subtype': 'plotter',         
        'event_description': 'Plotter has finished plotting a sector'
    },
    {
        'event_name': 'New Cache Discovered',                          
        'event_type': 'farmer',     
        'event_subtype': 'controller',      
        'event_description': 'Controller has discovered a new cache'
    },
    {
        'event_name': 'Discovered New Farm',                           
        'event_type': 'farmer',     
        'event_subtype': 'controller',      
        'event_description': 'Controller has discovered a new farm'
    },
    {
        'event_name': 'Farm Initialized Successfully',                 
        'event_type': 'farmer',     
        'event_subtype': 'controller',      
        'event_description': 'Controller initialized a new farm successfully'
    },
    {
        'event_name': 'Idle',                                          
        'event_type': 'node',       
        'event_subtype': 'node',            
        'event_description': 'Node is synced'
    },
    {
        'event_name': 'Preparing',                                     
        'event_type': 'node',       
        'event_subtype': 'node',            
        'event_description': 'Node is preparing to sync'
    },
    {
        'event_name': 'Syncing',                                       
        'event_type': 'node',       
        'event_subtype': 'node',            
        'event_description': 'Node is syncing'
    },
    {
        'event_name': 'Pending',                                       
        'event_type': 'node',       
        'event_subtype': 'node',            
        'event_description': 'Node is in a pending state'
    },
    {
        'event_name': 'Claimed',                                       
        'event_type': 'node',       
        'event_subtype': 'node',            
        'event_description': 'Node has claimed a reward'
    }
]