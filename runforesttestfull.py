from forest import *
from forest.bobs.Bobs import *

import forest.engines.Config

# Debugger line
#import pdb; pdb.set_trace()

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
    raster = GeotiffRead(filename = datafilename)    
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
        print(zone,"=",zonalaverage.data[zone])
    
    print("finished old school")

    
def zonalaverage_forest(zonefilename, datafilename):

    #output = run_primitive( VectorZoneTest.reg(zonefilename) == RasterDataTest.reg(datafilename) < PartialSum > AggregateSum == Average )
    output = run_primitive( ShapefileNewRead.reg(zonefilename) == GeotiffRead.reg(datafilename) < PartialSumRasterize > AggregateSum == Average )

    return output
    
    
def testit_forest(zonefilename, datafilename):
    print("starting forest")
    
    zonalaverage = zonalaverage_forest(zonefilename, datafilename)
    
    print("ZonalAverage=",zonalaverage)
    
    for zone in sorted(zonalaverage.data):
        print(zone,"=",zonalaverage.data[zone])
    
    print("finished forest")


    
if __name__ == '__main__':
    
    zonefilename = "examples/data/states.shp"
    datafilename = "examples/data/glc2000.tif"

    testit_forest(zonefilename,datafilename)
