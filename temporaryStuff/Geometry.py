import math


class Point:
    def __init__(self, x, y):
        self.x = x
        self.y = y
    
    def __str__(self):
        return f'({self.x}|{self.y})'


class LineSegment:
    def __init__(self, a: Point, b: Point):
        self.first = a
        self.second = b

    def getBoundingBox(self):
        result = [None] * 2
        result[0] = Point(math.min(self.first.x, self.second.x),
                          math.min(self.first.y, self.second.y))
        result[1] = Point(math.max(self.first.x, self.second.x),
                          math.max(self.first.y, self.second.y))
        return result
    
    def __str__(self):
        return f'LineSegment [{self.first} to {self.second}]'


class Geometry:
    EPSILON = 0.000001

    def crossProduct(self, a: Point, b: Point):
        '''
        Calculate the cross product of two points.\n
        @param a first point\n
        @param b second point\n
        @return the value of the cross product
        '''
        return a.x * b.y - b.x * a.y

    def doBoundingBoxesIntersect(self, a: list, b: list) -> bool:
        '''
        Check if bounding boxes do intersect. If one bounding box\n
        touches the other, they do intersect.\n
        @param a first bounding box\n
        @param b second bounding box\n
        @return true if they intersect,\n
                false otherwise.
        '''
        return a[0].x <= b[1].x and a[1].x >= b[0].x and a[0].y <= b[1].y \
               and a[1].y >= b[0].y

    def isPointOnLine(self, a: LineSegment, b: Point) -> bool:
        '''
        Checks if a Point is on a line\n
        @param a line (interpreted as line, although given as line\n
                       segment)\n
        @param b point\n
        @return true if point is on line, otherwise\n
                false
        '''
        # Move the image, so that a.first is on (0|0)
        aTmp = LineSegment(Point(0, 0), Point(a.second.x - a.first.x, a.second.y - a.first.y))
        bTmp = Point(b.x - a.first.x, b.y - a.first.y)
        r = self.crossProduct(aTmp.second, bTmp)
        return math.abs(r) < EPSILON

    def isPointRightOfLine(self, a: LineSegment, b: Point) -> bool:
        '''
        Checks if a point is right of a line. If the point is on the
        line, it is not right of the line.
        @param a line segment interpreted as a line
        @param b the point
        @return true if the point is right of the line,
                false otherwise
        '''
        # Move the image, so that a.first is on (0|0)
        aTmp = LineSegment(Point(0, 0), Point(a.second.x - a.first.x, a.second.y - a.first.y))
        bTmp = Point(b.x - a.first.x, b.y - a.first.y)
        return self.crossProduct(aTmp.second, bTmp) < 0

    def lineSegmentTouchesOrCrossesLine(self, a: LineSegment, b: LineSegment) -> bool:
        '''
        Check if line segment first touches or crosses the line that is\n
        defined by line segment second.\n
        @param first line segment interpreted as line\n
        @param second line segment\n
        @return true if line segment first touches or\n
                                  crosses line second,\n
                false otherwise.
        '''
        return self.isPointOnLine(a, b.first) or self.isPointOnLine(a, b.second) \
               or (self.isPointRightOfLine(a, b.first) ^ self.isPointRightOfLine(a, b.second))
    
    @staticmethod
    def doLinesIntersect(a: LineSegment, b: LineSegment) -> bool:
        '''
        Check if line segments intersect\n
        @param a first line segment\n
        @param b second line segment\n
        @return true if lines do intersect,\n
                false otherwise
        '''
        box1 = a.getBoundingBox()
        box2 = b.getBoundingBox()
        return doBoundingBoxesIntersect(box1, box2) \
               and lineSegmentTouchesOrCrossesLine(a, b) \
               and lineSegmentTouchesOrCrossesLine(b, a)



if __name__ == "__main__":
    test1 = LineSegment(Point(1,1), Point(4,4))
    test2 = LineSegment(Point(1,4), Point(4,1))
    test3 = Geometry.doLinesIntersect(test1, test2)
    print(test3)
