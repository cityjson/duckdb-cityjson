<?xml version="1.0" encoding="utf-8"?>
<!-- CityGML 3.0 test file: single Building with LOD2, boundary surfaces, and attributes -->
<core:CityModel xmlns:core="http://www.opengis.net/citygml/3.0"
	xmlns:bldg="http://www.opengis.net/citygml/building/3.0"
	xmlns:gml="http://www.opengis.net/gml/3.2"
	xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
	<gml:boundedBy>
		<gml:Envelope srsDimension="3" srsName="urn:ogc:def:crs:EPSG::25832">
			<gml:lowerCorner>100.0 200.0 0.0</gml:lowerCorner>
			<gml:upperCorner>110.0 210.0 10.0</gml:upperCorner>
		</gml:Envelope>
	</gml:boundedBy>
	<core:cityObjectMember>
		<bldg:Building gml:id="building_v3_1">
			<bldg:yearOfConstruction>2020</bldg:yearOfConstruction>
			<bldg:measuredHeight uom="#m">12.5</bldg:measuredHeight>
			<bldg:storeysAboveGround>3</bldg:storeysAboveGround>
			<bldg:function>1000</bldg:function>
			<bldg:boundedBy>
				<bldg:GroundSurface gml:id="v3_ground1">
					<bldg:lod2MultiSurface>
						<gml:MultiSurface>
							<gml:surfaceMember>
								<gml:Polygon gml:id="v3_poly_ground1">
									<gml:exterior>
										<gml:LinearRing>
											<gml:posList>100.0 200.0 0.0 110.0 200.0 0.0 110.0 210.0 0.0 100.0 210.0 0.0 100.0 200.0 0.0</gml:posList>
										</gml:LinearRing>
									</gml:exterior>
								</gml:Polygon>
							</gml:surfaceMember>
						</gml:MultiSurface>
					</bldg:lod2MultiSurface>
				</bldg:GroundSurface>
			</bldg:boundedBy>
			<bldg:boundedBy>
				<bldg:WallSurface gml:id="v3_wall_south">
					<bldg:lod2MultiSurface>
						<gml:MultiSurface>
							<gml:surfaceMember>
								<gml:Polygon gml:id="v3_poly_wall_south">
									<gml:exterior>
										<gml:LinearRing>
											<gml:posList>100.0 200.0 0.0 110.0 200.0 0.0 110.0 200.0 10.0 100.0 200.0 10.0 100.0 200.0 0.0</gml:posList>
										</gml:LinearRing>
									</gml:exterior>
								</gml:Polygon>
							</gml:surfaceMember>
						</gml:MultiSurface>
					</bldg:lod2MultiSurface>
				</bldg:WallSurface>
			</bldg:boundedBy>
			<bldg:boundedBy>
				<bldg:RoofSurface gml:id="v3_roof1">
					<bldg:lod2MultiSurface>
						<gml:MultiSurface>
							<gml:surfaceMember>
								<gml:Polygon gml:id="v3_poly_roof1">
									<gml:exterior>
										<gml:LinearRing>
											<gml:posList>100.0 200.0 10.0 110.0 200.0 10.0 110.0 210.0 10.0 100.0 210.0 10.0 100.0 200.0 10.0</gml:posList>
										</gml:LinearRing>
									</gml:exterior>
								</gml:Polygon>
							</gml:surfaceMember>
						</gml:MultiSurface>
					</bldg:lod2MultiSurface>
				</bldg:RoofSurface>
			</bldg:boundedBy>
		</bldg:Building>
	</core:cityObjectMember>
</core:CityModel>
