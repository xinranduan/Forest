"""
Copyright (c) 2017 Eric Shook. All rights reserved.
Use of this source code is governed by a BSD-style license that can be found in the LICENSE file.
@author: eshook (Eric Shook, eshook@gmail.edu)
@contributors: (Luyi Hunter, chen3461@umn.edu; Xinran Duan, duanx138@umn.edu)
@contributors: <Contribute and add your name here!>
"""

from ..bobs.Bobs import *
from . import Config
import math
import multiprocessing
import gdal

# new import for spark
import avro
from avro.datafile import DataFileWriter
from avro.io import DatumWriter
import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--jars spark-avro_2.11-4.0.0.jar pyspark-shell'
from pyspark.sql import SparkSession
import sys
import pickle

class Engine(object):
    def __init__(self, engine_type):
        self.engine_type = engine_type # Describes the type of engine
        self.is_split = False # The data are not split at the beginning
        
    def __repr___(self):
        return "Engine "+str(self.engine_type)
        
    # Split (<)
    def split(self, bobs):
        pass
    
    # Merge (>)
    def merge(self, bobs):
        pass

    # Sequence (==)
    def sequence(self, bobs):
        pass
    
    # Synchronization (!=)
    def synchronization(self, bobs):
        pass
    
    # Cycle start (<<)
    def cycle_start(self, bobs):
        pass
    
    # Cycle termination (>>)
    def cycle_termination(self, bobs):
        pass

    # This method will run a single primitive operation
    # It will pull data from inputs and run the primitive
    # It will save the input
    def run(self, primitive):
        print("Running", primitive)

        # Get the name of the primitive operation being executed
        name = primitive.__class__.__name__

        # Get the inputs        
        inputs = Config.inputs
    
        # Save the flows information in the global config data structure
        # FIXME: The problem with this solution is all data will be stored
        #        indefinitely, which is going to be a huge problem.
        Config.flows[name] = {}
        Config.flows[name]['input'] = inputs   
    
        # If Bobs are not split, then it is easy
        if Config.engine.is_split is False:

            if isinstance(inputs,Bob):     # If it is a bob
                inputs = primitive(inputs)    # Just pass in the bob
            else:                          # If it is a list
                inputs = primitive(*inputs)   # De-reference the list and pass as parameters
        
        else: # When they are split we have to handle the list of Bobs
            new_inputs = []
            # Loop over the split bobs
            
            for splitbobs in inputs:
                out = None # Record output from primitive
                if isinstance(splitbobs,Bob): # If it is a bob
                    out = primitive(splitbobs)       # Just pass in the bob
                else:                         # If it is a list
                    out = primitive(*splitbobs)      # De-reference the list and pass as parameters
                new_inputs.append(out) # Save the output in the new_inputs list
            inputs = new_inputs
        
        # Save the outputs from this primitive
        Config.flows[name]['output'] = inputs
        
        # Save inputs from this/these primitive(s), for the next primitive
        if primitive.passthrough is False: # Typical case
            Config.inputs = inputs # Reset the inputs
        else:
            assert(Config.engine.is_split is False)
            Config.inputs.append(inputs) # Add to the inputs
            
        return inputs

# FIXME: Change to Engines.py    
class PassEngine(Engine):
    def __init__(self):
        # FIXME: Need an object to describe type of engines rather than a string
        super(PassEngine,self).__init__("PassEngine")
    
# This is the default engine that doesn't do anything.
pass_engine = PassEngine()    

# FIXME: Change to Engines.py    
class TileEngine(Engine):
    def __init__(self):
        # FIXME: Need an object to describe type of engines rather than a string
        super(TileEngine,self).__init__("TileEngine")
        self.is_split=False
        
    # Split (<)
    
    # FIXME: Split also has to reach into Config.flows in case if functions pull out of list
    # Keeping a nested open/bound variable stack might be easier than flows
    # Think this one through
    
    def split(self, bobs):
        
        # Set the number of tiles to split to
        # FIXME: Eventually this should be determined or user-defined.
        num_tiles = Config.n_tile
        print("-> Number of tiles = ", num_tiles)
        
        # If already split, do nothing.
        if self.is_split is True:
            return
        
        new_inputs = []
        # Loop over bobs in inputs to split
        for bob in Config.inputs:
            
            # For each bob, create a new split tile (TileEngine :)
            tiles = []
            
            # Split only works for rasters for now
            # For all other data types (e.g., vectors) we just duplicate the data
            if not isinstance(bob,Raster): # Check if not a raster
                for tile_index in range(num_tiles):
                    tiles.append(bob) # Just copy the entire bob to a tile list
                    ######FIX ME: Fetch vector data later in worker#######
                    # tiles.append('vector')
                    ###+++++++++++++++++++++++++++++++++++++++++++++++++##
                    
                new_inputs.append(tiles) # Now add the tiles to new_inputs
                continue # Now skip to the next bob in the list

            # This code will only be reached for Raster data types
            assert(isinstance(bob,Raster))
            # Sanity check, if tiles are larger than data
            if num_tiles > bob.nrows:
                num_tiles = bob.nrows # Reset to be 1 row per tile
            
            # Set tile nrows and ncols
            tile_nrows = math.ceil(bob.nrows / num_tiles)
            tile_ncols = bob.ncols
            
            for tile_index in range(num_tiles):
                # Calculate the r,c location
                tile_r = tile_nrows * tile_index
                tile_c = 0
                
                # For the last tile_index, see if we are "too tall"
                # Meaning that the tiles are larger than the bob itself
                #  split Bob size      > Actual bob size
                if tile_r + tile_nrows > bob.nrows:
                    # If so, then resize so it is correct to bob.nrows
                    tile_nrows = bob.nrows - tile_r
                
                # Set tile height and width
                tile_h = bob.cellsize * tile_nrows
                tile_w = bob.w

                # Calculate y,x
                tile_y = bob.y + tile_r * bob.cellsize
                tile_x = bob.x
                
                # Create the tile
                tile = Bob(tile_y,tile_x,tile_h,tile_w)
                tile.nrows = tile_nrows
                tile.ncols = tile_ncols
                tile.r =     tile_r
                tile.c =     tile_c
                tile.cellsize = bob.cellsize
                tile.datatype = bob.datatype
                
                ######################################################
                ## Copy filename from Raster Bob to each tile
                tile.filename = bob.filename 
                tile.nodatavalue = bob.nodatavalue
                
                # UPDATE: No data passed on here. Read them in worker.
                # Split the data (depends on raster/vector)
                # tile.data = bob.get_data(tile_r,tile_c,tile_nrows,tile_ncols)
                ######################################################
                                
                # Save tiles
                tiles.append(tile)
            # Save list of tiles (split Bobs) to new inputs
            # Notice that they are not grouped as inputs
            # So they will need to be zipped
            new_inputs.append(tiles)
                    
        # Now we have new_inputs so rewrite Config.inputs with new list
        # Zip the list to create groups of split bobs
        # These groups will be input for the primitives
        zip_inputs = zip(*new_inputs)
        Config.inputs = list(zip_inputs) # Dereference zip object and create a list
        
        # Set split to True so engine knows that Config.inputs is split                
        self.is_split = True 
        
    # Merge (>)
    def merge(self, bobs):
        # Now that everything is merged set split to be false
        self.is_split = False

    # Sequence (==)
    def sequence(self, bobs):
        # If the Bobs are split, then handle it
        # If they are not, then there is nothing to do
        if self.is_split is True:
            # FIXME: Need to handle this
            print("NEED TO LOOP OVER SPLIT BOBS")
            pass # Loop over all the split Bobs
        pass

    
tile_engine = TileEngine()

# This worker is used for parallel execution in the multiprocessing engine    
def worker(input_list):

    rank = input_list[0]      # Rank
    iq = input_list[1]        # Input queue
    oq = input_list[2]        # Output queue
    primitive = input_list[3] # Primitive to run

    # Get the split bobs to process
    splitbobs = iq.get()
    
    ######################################################
    tile = splitbobs[1]
    filehandle = gdal.Open(tile.filename)
    band = filehandle.GetRasterBand(1)
    reverse_rnum = filehandle.RasterYSize-(tile.r+tile.nrows)
    tile.data = band.ReadAsArray(tile.c,reverse_rnum,tile.ncols,tile.nrows)
    ######################################################
    
    ######FIX ME: Fetch vector data (does not work for now)########
    # vector_data = []
    # for bob in Config.inputs:
    #     if not isinstance(bob,Raster):
    #         vector_data.append(bob)
    #         break
    #     else:
    #         continue
    # # Run the primitive on the splitbobs, record the output
    # out = primitive(vector_data[0], tile)
    ######+++++++++++++++++++++++++++++++++++++++++++++++++########
        
    # Run the primitive on the splitbobs, record the output
    out = primitive(splitbobs[0], tile)

    ######################################################
    ## delete the tile.data before passing output 
    del tile
    tile = None
    ######################################################

    oq.put(out) # Save the output in the output queue
    # return "worker %d %s %d %s" % (rank, splitbobs, test_number, str(tiletype)) 
    return "worker %d %s" % (rank, splitbobs)  
    
# FIXME: Change to Engines.py    
class MultiprocessingEngine(Engine):
    def __init__(self):
        # FIXME: Need an object to describe type of engines rather than a string
        super(MultiprocessingEngine,self).__init__("MultiprocessingEngine")
        self.is_split=False
        
    def split(self, bobs):
        # Run the split from the TileEngine
        # That will provide a list of bobs in inputs to parallelize
        tile_engine.split(bobs)
        self.is_split = True
        
    # Merge (>)
    def merge(self, bobs):
        # Now that everything is merged set split to be false
        self.is_split = False

    # Sequence (==)
    def sequence(self, bobs):
        # If the Bobs are split, then handle it
        # If they are not, then there is nothing to do
        if self.is_split is True:
            # FIXME: Need to handle this
            print("NEED TO LOOP OVER SPLIT BOBS")
            pass # Loop over all the split Bobs
        pass

    # This method changes the run behavior to be in parallel.
    def run(self, primitive):
        print("Running", primitive)

        # Get the name of the primitive operation being executed
        name = primitive.__class__.__name__

        # Get the inputs        
        inputs = Config.inputs
    
        # Save the flows information in the global config data structure
        # FIXME: The problem with this solution is all data will be stored
        #        indefinitely, which is going to be a huge problem.
        Config.flows[name] = {}
        Config.flows[name]['input'] = inputs   

        # If Bobs are not split, then it is easy
        if Config.engine.is_split is False:
            if isinstance(inputs,Bob):     # If it is a bob
                inputs = primitive(inputs)    # Just pass in the bob
            else:                          # If it is a list
                inputs = primitive(*inputs)   # De-reference the list and pass as parameters
        
        else: # When they are split we have to handle the list of Bobs and run in parallel

            # Make a pool of 4 processes
            # FIXME: THIS IS FIXED FOR NOW
            # print("-> Number of processes = ", Config.n_core)

            pool = multiprocessing.Pool(Config.n_core)
            
            # Create a manager for the input and output queues (iq, oq)  
            m = multiprocessing.Manager()
            iq = m.Queue()
            oq = m.Queue()
            
            # Add split bobs to the input queue to be processed
            for splitbobs in inputs:
                iq.put(splitbobs)
            
            # How many times will we run the worker function using map
            mapsize = len(inputs)

            # Make a list of ranks, queues, and primitives
            # These will be used for map_inputs
            ranklist = range(mapsize)
            iqlist = [iq for i in range(mapsize)]
            oqlist = [oq for i in range(mapsize)]
            prlist = [primitive for i in range(mapsize)]
            
            # Create map inputs by zipping the lists we just created
            map_inputs = zip(ranklist,iqlist,oqlist,prlist)
                       
            # Apply the inputs to the worker function using parallel map
            # Results can be printed for output from the worker tasks
            results = pool.map(worker, map_inputs)

            # Get the outputs from the output queue and save as new inputs
            inputs = []
            while not oq.empty():
                output = oq.get() # Get one output from the queue
                inputs.append(output) # Save to inputs
            
            # Done with the pool so close, then join (wait)
            pool.close()
            pool.join()
            
        # Save the outputs from this primitive
        Config.flows[name]['output'] = inputs
        
        # Save inputs from this/these primitive(s), for the next primitive
        if primitive.passthrough is False: # Typical case
            Config.inputs = inputs # Reset the inputs
        else:
            assert(Config.engine.is_split is False)
            Config.inputs.append(inputs) # Add to the inputs
            
        return inputs

mp_engine = MultiprocessingEngine()

class SparkEngine(Engine):
    def __init__(self):
        super(SparkEngine, self).__init__("SparkEngine")
        self.is_split = False

    def split(self, bobs):

        # Set the number of tiles to split to
        num_tiles = Config.n_tile
        print("-> Number of tiles = ", num_tiles)

        AVRO_FILENAME = "spark_tiles.avro";

        # If already split, do nothing.
        if self.is_split is True:
            return

        # counters for bob types
        raster_count = 0;
        nonraster_count = 0;

        USE_NEW_SPLIT = True
        if (USE_NEW_SPLIT):
            # New Split
            # Step 1: Collect all non-raster bobs as bytes (e.g. shpfiles vector)

            nonraster_bob_list = list(filter(lambda bob: not isinstance(bob, Raster), Config.inputs));
            nonraster_count = len(nonraster_bob_list);
            print("# of non-Raster Bobs:", nonraster_count);
            nonraster_bytes_list = [];

            # loop over all non-raster bobs
            for nonraster_bob in nonraster_bob_list:

                # Convert non raster bob into bytes
                nonraster_bytes = pickle.dumps(nonraster_bob);
                nonraster_bytes_list.append(nonraster_bytes);

            # End of Step 1

            # Step 2: Collect all raster bobs (e.g. geotiff raster)

            rasterbob_list = list(filter(lambda bob: isinstance(bob, Raster), Config.inputs));
            raster_count = len(rasterbob_list);
            print("# of Raster Bobs:", raster_count);

            # create Avro schema and data file
            if nonraster_count == 1 and raster_count == 1:
                # Examples: zonal operations
                schema_str = OneGeotiffOneShpAvroSchema();
            elif nonraster_count == 0 and raster_count == 2:
                # Examples: basic operations, NDVI
                schema_str = TwoGeotiffAvroSchema();
            elif nonraster_count == 0 and raster_count == 1:
                # Examples: HillShade
                schema_str = OneGeotiffAvroSchema()
            else:
                print("*** PLEASE CREATE NEW AVRO SCHEMA! ***")
                assert(False);

            schema = avro.schema.Parse(schema_str)
            avro_writer = DataFileWriter(open(AVRO_FILENAME, "wb"), DatumWriter(), schema)

            # Create a list of reusable filehandles
            filehandle_list = [];
            for bob in rasterbob_list:
                filehandle_list.append(gdal.Open(bob.filename));

            # Compute each Spark tile
            for tile_index in range(num_tiles):
                print("Tile #", tile_index);

                # contains i-th subtiles from each raster bob
                raster_subtile = [];

                # loop over raster bobs to get i-th subtile
                for bob_index in range(raster_count):
                    print("\tSubtile #", bob_index);
                    bob = rasterbob_list[bob_index];
                    filehandle = filehandle_list[bob_index];

                    # Sanity check, if tiles are larger than data
                    if num_tiles > bob.nrows:
                        num_tiles = bob.nrows  # Reset to be 1 row per tile

                    # Set tile nrows and ncols
                    tile_nrows = math.ceil(bob.nrows / num_tiles)
                    tile_ncols = bob.ncols

                    # Calculate the r,c location
                    tile_r = tile_nrows * tile_index
                    tile_c = 0

                    # For the last tile_index, see if we are "too tall"
                    # Meaning that the tiles are larger than the bob itself
                    #  split Bob size      > Actual bob size
                    if tile_r + tile_nrows > bob.nrows:
                        # If so, then resize so it is correct to bob.nrows
                        tile_nrows = bob.nrows - tile_r

                    # Set tile height and width
                    tile_h = bob.cellsize * tile_nrows
                    tile_w = bob.w

                    # Calculate y,x
                    tile_y = bob.y + tile_r * bob.cellsize
                    tile_x = bob.x

                    # Create the tile
                    subtile = Bob(tile_y, tile_x, tile_h, tile_w)
                    subtile.nrows = tile_nrows
                    subtile.ncols = tile_ncols
                    subtile.r = tile_r
                    subtile.c = tile_c
                    subtile.cellsize = bob.cellsize
                    subtile.datatype = bob.datatype

                    subtile.filename = bob.filename
                    subtile.nodatavalue = bob.nodatavalue

                    band = filehandle.GetRasterBand(1)
                    reverse_rnum = filehandle.RasterYSize - (subtile.r + subtile.nrows)
                    subtile.data = band.ReadAsArray(subtile.c, reverse_rnum, subtile.ncols, subtile.nrows)
                    print("\t-> data shape:", subtile.data.shape)
                    print("\t-> data type:", subtile.data.dtype)

                    raster_subtile.append(subtile);
                    # End of each subtile processing

                if nonraster_count == 1 and raster_count == 1:
                    # Examples: zonal operations
                    shp_bytes = nonraster_bytes_list[0]; # should only have one shp
                    tile = raster_subtile[0]; # should only have one subtile
                    avro_writer.append({"data": pickle.dumps(tile.data), \
                                        "zone": shp_bytes, \
                                        "nodatavalue": tile.nodatavalue, \
                                        "x": tile.x, \
                                        "y": tile.y, \
                                        "h": tile.h, \
                                        "cellsize": tile.cellsize})

                elif nonraster_count == 0 and raster_count == 2:
                    # Examples: basic operations, NDVI
                    tile1 = raster_subtile[0];
                    tile2 = raster_subtile[1];
                    avro_writer.append({"data1": pickle.dumps(tile1.data), \
                                        "data2": pickle.dumps(tile2.data), \
                                        "nodatavalue1": tile1.nodatavalue, \
                                        "nodatavalue2": tile2.nodatavalue})

                elif nonraster_count == 0 and raster_count == 1:
                    # Examples: HillShade
                    tile = raster_subtile[0];  # should only have one subtile
                    avro_writer.append({"data": pickle.dumps(tile.data), \
                                        "nodatavalue": tile.nodatavalue})
                else:
                    print("*** PLEASE CREATE NEW AVRO SCHEMA! ***")
                    assert (False);

            # End of Step 2
            # End of New Split

        else:
            # Old Split
            for bob in Config.inputs:

                # For all other data types (e.g., vectors) we just duplicate the data
                # CASE : shapefile
                if not isinstance(bob, Raster):
                    nonraster_count += 1

                    # Convert shapefile bob into bytes
                    shp_bytes = pickle.dumps(bob)

                    continue  # Now skip to the next bob in the list

                # DEBUG
                assert (isinstance(bob, Raster))

                # CASE: geotiff
                raster_count += 1

                # Sanity check, if tiles are larger than data
                if num_tiles > bob.nrows:
                    num_tiles = bob.nrows  # Reset to be 1 row per tile

                # Set tile nrows and ncols
                tile_nrows = math.ceil(bob.nrows / num_tiles)
                tile_ncols = bob.ncols

                # create Avro schema and data file
                schema = avro.schema.Parse(OneGeotiffOneShpAvroSchema())
                avro_writer = DataFileWriter(open(AVRO_FILENAME, "wb"), DatumWriter(), schema)

                # open geotiff file
                filehandle = gdal.Open(bob.filename)

                # Compute each tile
                for tile_index in range(num_tiles):

                    print("Tile #", tile_index)
                    # Calculate the r,c location
                    tile_r = tile_nrows * tile_index
                    tile_c = 0

                    # For the last tile_index, see if we are "too tall"
                    # Meaning that the tiles are larger than the bob itself
                    #  split Bob size      > Actual bob size
                    if tile_r + tile_nrows > bob.nrows:
                        # If so, then resize so it is correct to bob.nrows
                        tile_nrows = bob.nrows - tile_r

                    # Set tile height and width
                    tile_h = bob.cellsize * tile_nrows
                    tile_w = bob.w

                    # Calculate y,x
                    tile_y = bob.y + tile_r * bob.cellsize
                    tile_x = bob.x

                    # Create the tile
                    tile = Bob(tile_y, tile_x, tile_h, tile_w)
                    tile.nrows = tile_nrows
                    tile.ncols = tile_ncols
                    tile.r = tile_r
                    tile.c = tile_c
                    tile.cellsize = bob.cellsize
                    tile.datatype = bob.datatype

                    tile.filename = bob.filename
                    tile.nodatavalue = bob.nodatavalue

                    # write title data to Avro
                    band = filehandle.GetRasterBand(1)
                    reverse_rnum = filehandle.RasterYSize - (tile.r + tile.nrows)
                    tile.data = band.ReadAsArray(tile.c, reverse_rnum, tile.ncols, tile.nrows)
                    print("-> data shape:", tile.data.shape)
                    print("-> data type:", tile.data.dtype)
                    avro_writer.append({"data": pickle.dumps(tile.data), \
                                        "zone": shp_bytes, \
                                        "nodatavalue": tile.nodatavalue, \
                                        "x": tile_x, \
                                        "y": tile_y, \
                                        "h": tile_h, \
                                        "cellsize": tile.cellsize})

                    # Save tiles
                    # tiles.append(tile)
                    # End of each tile

                # Save list of tiles (split Bobs) to new inputs
                # Notice that they are not grouped as inputs
                # So they will need to be zipped
                # new_inputs.append(tiles)
                avro_writer.close()
                # End of for each bob (CASE Geotiff)
                # End of Old Split

        # Read Avro file into Spark
        spark = SparkSession \
            .builder \
            .appName("Python Spark SQL basic example") \
            .master("local") \
            .config("spark.sql.avro.compression.codec", "deflate") \
            .config('spark.hadoop.avro.mapred.ignore.inputs.without.extension', 'false') \
            .getOrCreate()
        rdd = spark.read.format("com.databricks.spark.avro").load(AVRO_FILENAME).rdd

        Config.inputs = rdd

        # Set split to True so engine knows that Config.inputs is split
        self.is_split = True

        # Merge (>)
    def merge(self, bobs):
        self.is_split = False

    # Sequence (==)
    def sequence(self, bobs):
        # If the Bobs are split, then handle it
        # If they are not, then there is nothing to do
        if self.is_split is True:
            # FIXME: Need to handle this
            print("NEED TO LOOP OVER SPLIT BOBS")
            pass  # Loop over all the split Bobs
        pass

    # This method changes the run behavior to be in parallel.
    def run(self, primitive):
        print("Running", primitive)

        # Get the name of the primitive operation being executed
        name = primitive.__class__.__name__

        # Get the inputs
        inputs = Config.inputs

        # Save the flows information in the global config data structure
        # FIXME: The problem with this solution is all data will be stored
        #        indefinitely, which is going to be a huge problem.
        Config.flows[name] = {}
        Config.flows[name]['input'] = inputs

        # If Bobs are not split, then it is easy
        if Config.engine.is_split is False:
            if isinstance(inputs, Bob):  # If it is a bob
                inputs = primitive(inputs)  # Just pass in the bob
            else:  # If it is a list
                inputs = primitive(*inputs)  # De-reference the list and pass as parameters

        else:  # When they are split we have to handle the list of Bobs and run in parallel

            print("-> in parallel")

            # inputs should be a rdd

            # DEBUG
            # cur_tile = inputs.take(1)
            # print("RDD title type:", type(cur_tile))

            # new rdd
            inputs = primitive(rdd=inputs);

            # DEBUG
            # new_tile = inputs.take(1)
            # print("New RDD title type:", type(new_tile))

            inputs = inputs.collect()

            # # TILE ENGINE SPLIT
            # pool = multiprocessing.Pool(Config.n_core)
            #
            # # Create a manager for the input and output queues (iq, oq)
            # m = multiprocessing.Manager()
            # iq = m.Queue()
            # oq = m.Queue()
            #
            # # Add split bobs to the input queue to be processed
            # for splitbobs in inputs:
            #     iq.put(splitbobs)
            #
            # # How many times will we run the worker function using map
            # mapsize = len(inputs)
            #
            # # Make a list of ranks, queues, and primitives
            # # These will be used for map_inputs
            # ranklist = range(mapsize)
            # iqlist = [iq for i in range(mapsize)]
            # oqlist = [oq for i in range(mapsize)]
            # prlist = [primitive for i in range(mapsize)]
            #
            # # Create map inputs by zipping the lists we just created
            # map_inputs = zip(ranklist, iqlist, oqlist, prlist)
            #
            # # Apply the inputs to the worker function using parallel map
            # # Results can be printed for output from the worker tasks
            # results = pool.map(worker, map_inputs)
            #
            # # Get the outputs from the output queue and save as new inputs
            # inputs = []
            # while not oq.empty():
            #     output = oq.get()  # Get one output from the queue
            #     inputs.append(output)  # Save to inputs
            #
            # # Done with the pool so close, then join (wait)
            # pool.close()
            # pool.join()

        # Save the outputs from this primitive
        Config.flows[name]['output'] = inputs

        # Save inputs from this/these primitive(s), for the next primitive
        if primitive.passthrough is False:  # Typical case
            Config.inputs = inputs  # Reset the inputs
        else:
            assert (Config.engine.is_split is False)
            Config.inputs.append(inputs)  # Add to the inputs

        return inputs

spark_engine = SparkEngine()

def OneGeotiffOneShpAvroSchema():
    return '{"namespace": "spark_tiles.avro",\n' \
           '"type": "record",\n ' \
           '"name": "SparkTiles",\n ' \
           '"fields": [\n' \
           '     {"name": "data", "type": "bytes"},\n' \
           '     {"name": "zone", "type": "bytes"},\n' \
           '     {"name": "nodatavalue",  "type": ["double", "null"]},\n' \
           '     {"name": "x", "type": ["float", "null"]},\n' \
           '     {"name": "y", "type": ["float", "null"]},\n' \
           '     {"name": "h", "type": ["float", "null"]},\n' \
           '     {"name": "cellsize", "type": ["float", "null"]}\n' \
           ' ]' \
           '\n}'

def TwoGeotiffAvroSchema():
    return '{"namespace": "spark_tiles.avro",\n' \
           '"type": "record",\n ' \
           '"name": "SparkTiles",\n ' \
           '"fields": [\n' \
           '     {"name": "data1", "type": "bytes"},\n' \
           '     {"name": "data2", "type": "bytes"},\n' \
           '     {"name": "nodatavalue1",  "type": ["double", "null"]},\n' \
           '     {"name": "nodatavalue2",  "type": ["double", "null"]}\n' \
           ' ]' \
           '\n}'

def OneGeotiffAvroSchema():
    return '{"namespace": "spark_tiles.avro",\n' \
           '"type": "record",\n ' \
           '"name": "SparkTiles",\n ' \
           '"fields": [\n' \
           '     {"name": "data", "type": "bytes"},\n' \
           '     {"name": "nodatavalue",  "type": ["double", "null"]}\n' \
           ' ]' \
           '\n}'

# Set the Config.engine as the default

Config.engine = tile_engine
Config.engine = pass_engine
Config.engine = mp_engine
Config.engine = spark_engine

print("Default engine",Config.engine)

if __name__ == '__main__':
    pass