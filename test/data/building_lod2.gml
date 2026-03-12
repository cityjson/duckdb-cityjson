<?xml version="1.0" encoding="utf-8"?>
<!-- CityGML 2.0 test file: single Building with LOD2, boundary surfaces, and attributes -->
<CityModel xmlns="http://www.opengis.net/citygml/2.0"
	xmlns:bldg="http://www.opengis.net/citygml/building/2.0"
	xmlns:gml="http://www.opengis.net/gml"
	xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
	xsi:schemaLocation="http://www.opengis.net/citygml/building/2.0 http://schemas.opengis.net/citygml/building/2.0/building.xsd">
	<gml:boundedBy>
		<gml:Envelope srsDimension="3" srsName="urn:ogc:def:crs:EPSG::25832">
			<gml:lowerCorner>100.0 200.0 0.0</gml:lowerCorner>
			<gml:upperCorner>110.0 210.0 10.0</gml:upperCorner>
		</gml:Envelope>
	</gml:boundedBy>
	<cityObjectMember>
		<bldg:Building gml:id="building1">
			<bldg:yearOfConstruction>1985</bldg:yearOfConstruction>
			<bldg:measuredHeight uom="#m">10.0</bldg:measuredHeight>
			<bldg:storeysAboveGround>2</bldg:storeysAboveGround>
			<bldg:function>1000</bldg:function>
			<bldg:boundedBy>
				<bldg:GroundSurface gml:id="ground1">
					<bldg:lod2MultiSurface>
						<gml:MultiSurface>
							<gml:surfaceMember>
								<gml:Polygon gml:id="poly_ground1">
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
				<bldg:WallSurface gml:id="wall_south">
					<bldg:lod2MultiSurface>
						<gml:MultiSurface>
							<gml:surfaceMember>
								<gml:Polygon gml:id="poly_wall_south">
									<gml:exterior>
										<gml:LinearRing>
											<gml:posList>100.0 200.0 0.0 110.0 200.0 0.0 110.0 200.0 8.0 100.0 200.0 8.0 100.0 200.0 0.0</gml:posList>
										</gml:LinearRing>
									</gml:exterior>
								</gml:Polygon>
							</gml:surfaceMember>
						</gml:MultiSurface>
					</bldg:lod2MultiSurface>
				</bldg:WallSurface>
			</bldg:boundedBy>
			<bldg:boundedBy>
				<bldg:WallSurface gml:id="wall_north">
					<bldg:lod2MultiSurface>
						<gml:MultiSurface>
							<gml:surfaceMember>
								<gml:Polygon gml:id="poly_wall_north">
									<gml:exterior>
										<gml:LinearRing>
											<gml:posList>110.0 210.0 0.0 100.0 210.0 0.0 100.0 210.0 8.0 110.0 210.0 8.0 110.0 210.0 0.0</gml:posList>
										</gml:LinearRing>
									</gml:exterior>
								</gml:Polygon>
							</gml:surfaceMember>
						</gml:MultiSurface>
					</bldg:lod2MultiSurface>
				</bldg:WallSurface>
			</bldg:boundedBy>
			<bldg:boundedBy>
				<bldg:WallSurface gml:id="wall_east">
					<bldg:lod2MultiSurface>
						<gml:MultiSurface>
							<gml:surfaceMember>
								<gml:Polygon gml:id="poly_wall_east">
									<gml:exterior>
										<gml:LinearRing>
											<gml:posList>110.0 200.0 0.0 110.0 210.0 0.0 110.0 210.0 8.0 110.0 200.0 8.0 110.0 200.0 0.0</gml:posList>
										</gml:LinearRing>
									</gml:exterior>
								</gml:Polygon>
							</gml:surfaceMember>
						</gml:MultiSurface>
					</bldg:lod2MultiSurface>
				</bldg:WallSurface>
			</bldg:boundedBy>
			<bldg:boundedBy>
				<bldg:WallSurface gml:id="wall_west">
					<bldg:lod2MultiSurface>
						<gml:MultiSurface>
							<gml:surfaceMember>
								<gml:Polygon gml:id="poly_wall_west">
									<gml:exterior>
										<gml:LinearRing>
											<gml:posList>100.0 210.0 0.0 100.0 200.0 0.0 100.0 200.0 8.0 100.0 210.0 8.0 100.0 210.0 0.0</gml:posList>
										</gml:LinearRing>
									</gml:exterior>
								</gml:Polygon>
							</gml:surfaceMember>
						</gml:MultiSurface>
					</bldg:lod2MultiSurface>
				</bldg:WallSurface>
			</bldg:boundedBy>
			<bldg:boundedBy>
				<bldg:RoofSurface gml:id="roof1">
					<bldg:lod2MultiSurface>
						<gml:MultiSurface>
							<gml:surfaceMember>
								<gml:Polygon gml:id="poly_roof1">
									<gml:exterior>
										<gml:LinearRing>
											<gml:posList>100.0 200.0 8.0 110.0 200.0 8.0 110.0 205.0 10.0 100.0 205.0 10.0 100.0 200.0 8.0</gml:posList>
										</gml:LinearRing>
									</gml:exterior>
								</gml:Polygon>
							</gml:surfaceMember>
							<gml:surfaceMember>
								<gml:Polygon gml:id="poly_roof2">
									<gml:exterior>
										<gml:LinearRing>
											<gml:posList>100.0 210.0 8.0 100.0 205.0 10.0 110.0 205.0 10.0 110.0 210.0 8.0 100.0 210.0 8.0</gml:posList>
										</gml:LinearRing>
									</gml:exterior>
								</gml:Polygon>
							</gml:surfaceMember>
						</gml:MultiSurface>
					</bldg:lod2MultiSurface>
				</bldg:RoofSurface>
			</bldg:boundedBy>
		</bldg:Building>
	</cityObjectMember>
</CityModel>
