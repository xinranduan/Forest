
# coding: utf-8

# In[ ]:

# %load runforesttestfull.py
#from memory_profiler import profile
from forest import *
from forest.bobs.Bobs import *
import forest.engines.Config

def testit_randomstuff():
    # Make an empty Bob
    b = Bob()
    print(b)

    # Make an empty raster dataset
    r = Raster(0,0,20,20,10,10,2)
    print(r)

def zonalaverage_oldschool(zonefilename, datafilename):
    
    # Read a rasterized Vector dataset as zones
    # FIXME: For now it does not read the file    
    vector = ShapefileNewRead(zonefilename)
    print("vector=",vector)
    print("data len=",len(vector.data))

    # Read a raster dataset as data
    # FIXME: For now it does not read the file    
    raster = GeotiffRead(datafilename)    
    print("raster=",raster)
    print("raster data len",len(raster.data))
    
    # Calculate partial sum of vector and raster
    ps = PartialSumRasterize(vector, raster)
    print("PartialSum=",ps)
    print(ps.data)    

    zonalaverage = Average(ps)

    return zonalaverage
    
def testit_oldschool(zonefilename, datafilename):

    print("starting old school")

    zonalaverage = zonalaverage_oldschool(zonefilename, datafilename)
    
    
    print("ZonalAverage=",zonalaverage)
    
    for zone in sorted(zonalaverage.data):
        print("  ",zone,"=",zonalaverage.data[zone])
    
    print("finished old school")

#@profile    
def zonalaverage_forest(zonefilename, datafilename):
    
    Config.inputs = []
    #output = run_primitive( VectorZoneTest.reg(zonefilename) == RasterDataTest.reg(datafilename) < PartialSum > AggregateSum == Average )
    output = run_primitive( ShapefileNewRead.reg(zonefilename) == GeotiffRead.reg(datafilename) < PartialSumRasterize > AggregateSum == Average )
    return output
    
    
def testit_forest(zonefilename, datafilename):
    print("starting forest")
    
    zonalaverage = zonalaverage_forest(zonefilename, datafilename)
    
    print("ZonalAverage=",zonalaverage) 
    
    for zone in sorted(zonalaverage.data):
        print("  ",zone,"=",zonalaverage.data[zone])
    
    print("finished forest")


zonefilename = "examples/data/states.shp"
datafilename = "examples/data/glc2000.tif"

# In[ ]:

# %prun testit_forest(zonefilename,datafilename)
# %lprun -T lprofoutput -f testit_forest testit_forest(zonefilename,datafilename)
# %mprun testit_forest(zonefilename,datafilename)

# import yappi
# yappi.start()

# yappi.get_func_stats().print_all()
# yappi.get_thread_stats().print_all()

# %load_ext line_profiler
# %lprun -f PartialSumRasterize.__call__ testit_forest(zonefilename,datafilename)

# %mprun -f MultiprocessingEngine.run testit_forest(zonefilename,datafilename)
# %mprun -f TileEngine.split testit_forest(zonefilename,datafilename)
# %mprun -f worker testit_forest(zonefilename,datafilename)
# %mprun -f GeotiffReadPrim.__call__ testit_forest(zonefilename,datafilename)
# from memory_profiler import profile
# mprof run --multiprocess testit_forest(zonefilename,datafilename)

def main():
    testit_forest(zonefilename,datafilename)

if __name__ == "__main__":
    main()

# %debug
# testit_forest(zonefilename,datafilename)




