from json import loads
from collections import namedtuple
import re

class AclIpSpace(namedtuple("AclIpSpace", "lines")):
    __slots__ = ()

    def to_str(self):
        return '\n'.join(map(to_str, self.lines))

    def simplify(self):
        lines = [ln.simplify() for ln in self.lines]

        # simplify to IpSpaceComplement
        if (len(lines) == 2 and
            lines[0].action == "DENY" and
            lines[1] == AclIpSpaceLine("PERMIT", UniverseIpSpace())):
            return IpSpaceComplement(lines[0].ipSpace)

        # simplify to IpSpaceIntersection
        if (len(lines) == 3 and
            lines[0].action == "DENY" and isinstance(lines[0].ipSpace, IpSpaceComplement) and
            lines[1].action == "DENY" and isinstance(lines[1].ipSpace, IpSpaceComplement) and
            lines[2] == AclIpSpaceLine("PERMIT", UniverseIpSpace())):
            return IpSpaceIntersection([lines[0].ipSpace.ipSpace, lines[1].ipSpace.ipSpace])

        return AclIpSpace(lines)


class AclIpSpaceLine(namedtuple("AclLine", "action ipSpace")):
    __slots__ = ()

    def to_str(self):
        return '%s %s' % (self.action, to_str(self.ipSpace))

    def simplify(self):
        if hasattr(self.ipSpace, "simplify"):
            return AclIpSpaceLine(self.action, self.ipSpace.simplify())
        return self


class IpSpaceComplement(namedtuple("IpSpaceComplement", "ipSpace")):
    __slots__ = ()

    def to_str(self):
        return "not " + self.ipSpace.to_str()


class IpSpaceUnion(namedtuple("IpSpaceUnion", "ipSpaces")):
    __slots__ = ()


class IpSpaceIntersection(namedtuple("IpSpaceIntersection", "ipSpaces")):
    __slots__ = ()

    def to_str(self):
        ipSpaces = set(self.ipSpaces)
        if len(ipSpaces) == 1:
            return list(ipSpaces)[0].to_str()
        return "intersect(%s)" % ', '.join([ipSpace.to_str() for ipSpace in self.ipSpaces])


class OrMatchExpr(namedtuple("OrMatchExpr", "disjuncts")):
    __slots__ = ()

    def to_explanations(self):
        return [disjunct.to_explanation() for disjunct in self.disjuncts]


class AndMatchExpr(namedtuple("AndMatchExpr", "conjuncts")):
    __slots__ = ()

    def to_explanation(self):
        return Explanation.from_conjucts(*self.conjuncts)

    def to_explanations(self):
        return [self.to_explanation()]


class IpIpSpace(namedtuple("IpIpSpace", "ip")):
    __slots__ = ()

    def to_str(self):
        return self.ip


class IpWildcard(namedtuple("IpWildcard", "ipWildcard")):
    __slots__ = ()

    def to_str(self):
        return self.ipWildcard


class IpWildcardIpSpace(namedtuple("IpWildcardIpSpace", "ipWildcard")):
    __slots__ = ()

    def to_str(self):
        return self.ipWildcard


class IpWildcardSetIpSpace(namedtuple("IpWildcardSetIpSpace", "whitelist")):
    __slots__ = ()

    def to_str(self):
        if len(self.whitelist) == 1:
            return self.whitelist[0]
        return to_str(self.whitelist)


class MatchHeaderSpace(namedtuple("MatchHeaderSpace", "headerSpace")):
    __slots__ = ()

    def to_explanation(self):
        return Explanation.from_conjucts(self)

    def to_explanations(self):
        return [self.to_explanation()]

    def to_str(self):
        headerSpace = self.headerSpace

        if headerSpace is None or len(headerSpace) == 0:
            return "full headerspace"
        else:
            if headerSpace.get('negate', None) is False:
                # copy before modifying
                headerSpace = dict(headerSpace)
                del headerSpace['negate']

        return ', '.join("%s: %s" % (renameHeaderSpaceField(key), to_str(val)) for (key, val) in headerSpace.items())


MatchSrcInterface = namedtuple("MatchSrcInterface", "srcInterfaces")


class NotMatchExpr(namedtuple("NotMatchExpr", "operand")):
    __slots__ = ()

    def to_explanation(self):
        return Explanation.from_conjucts(self)


class OriginatingFromDevice(namedtuple("OriginatingFromDevice", "")):

    def to_explanation(self):
        return Explanation.from_conjucts(self)

    def to_explanations(self):
        return [self.to_explanation()]


class SubRange(namedtuple("SubRange", "start end")):
    __slots__ = ()

    def to_str(obj):
        if obj.start == obj.end:
            return str(obj.start)
        return '%s to %s' % (obj.start, obj.end)


class TrueExpr(namedtuple("TrueExpr", "")):
    __slots__  = ()

    def to_explanation(self):
        return Explanation.from_conjucts()

    def to_explanations(self):
        return [self.to_explanation()]


class UnionIpSpace(namedtuple("UnionIpSpace", "ipSpaces")):
    __slots__ = ()

    def to_str(self):
        spaces = list(self.ipSpaces)
        if len(spaces) == 1:
            return spaces[0].to_str()
        return 'union(%s)' % ', '.join((space.to_str() for space in spaces))


class UniverseIpSpace(namedtuple("UniverseIpSpace", "")):
    __slots__ = ()

    def to_str(self):
        return "any"


aclLineMatchExprParsers = {}


def parse(obj):
    if isinstance(obj, dict):
        return parseObj(obj)
    elif isinstance(obj, str):
        return parseStr(obj)
    elif isinstance(obj, list):
        return list(map(parse, obj))
    else:
        return obj


def parseObj(obj):
    try:
        parser = aclLineMatchExprParsers[obj['class']]
    except KeyError:
        return obj

    return parser(obj)



def parseStr(s):
    if re.compile("\d+-\d+").fullmatch(s):
        [start, end] = s.split('-')
        return SubRange(int(start), int(end))

    return s


# Object parsers
def parseAclIpSpace(obj):
    lines = [AclIpSpaceLine(line['action'], parse(line['ipSpace'])) for line in obj['lines']]

    if all(map(lambda ln: ln.action == 'PERMIT', lines)):
        return UnionIpSpace(map(lambda ln: ln.ipSpace, lines))

    return AclIpSpace(lines)


aclLineMatchExprParsers["org.batfish.datamodel.AclIpSpace"] = parseAclIpSpace


def parseAndMatchExpr(obj):
    return AndMatchExpr([parse(conjunct) for conjunct in obj['conjuncts']])


aclLineMatchExprParsers["org.batfish.datamodel.acl.AndMatchExpr"] = parseAndMatchExpr


def parseIpIpSpace(obj):
    return IpIpSpace(obj['ip'])


aclLineMatchExprParsers["org.batfish.datamodel.IpIpSpace"] = parseIpIpSpace


def parseIpWildcard(obj):
    return IpWildcard(obj['ipWildcard'])


aclLineMatchExprParsers["org.batfish.datamodel.IpWildcard"] = parseIpWildcard


def parseIpWildcardIpSpace(obj):
    return IpWildcardIpSpace(parse(obj['ipWildcard']))


aclLineMatchExprParsers["org.batfish.datamodel.IpWildcardIpSpace"] = parseIpWildcardIpSpace


def parseIpWildcardSetIpSpace(obj):
    return IpWildcardSetIpSpace(parse(obj['whitelist']))


aclLineMatchExprParsers["org.batfish.datamodel.IpWildcardSetIpSpace"] = parseIpWildcardSetIpSpace


def parseOrMatchExpr(obj):
    return OrMatchExpr([parse(disjunct) for disjunct in obj['disjuncts']])


aclLineMatchExprParsers["org.batfish.datamodel.acl.OrMatchExpr"] = parseOrMatchExpr


def parseNotMatchExpr(obj):
    return NotMatchExpr(parse(obj['operand']))


aclLineMatchExprParsers["org.batfish.datamodel.acl.NotMatchExpr"] = parseNotMatchExpr


def parseOriginatingFromDevice(obj):
    return OriginatingFromDevice()


aclLineMatchExprParsers["org.batfish.datamodel.acl.OriginatingFromDevice"] = parseOriginatingFromDevice


def parseMatchHeaderSpace(obj):
    constraints = {}
    for (nm, val) in obj['headerSpace'].items():
        constraints[nm] = parse(val)
    return MatchHeaderSpace(constraints)


aclLineMatchExprParsers["org.batfish.datamodel.acl.MatchHeaderSpace"] = parseMatchHeaderSpace


def parseMatchSrcInterface(obj):
    return MatchSrcInterface(obj['srcInterfaces'])


aclLineMatchExprParsers["org.batfish.datamodel.acl.MatchSrcInterface"] = parseMatchSrcInterface


def parseTrueExpr(obj):
    return TrueExpr()


aclLineMatchExprParsers["org.batfish.datamodel.acl.TrueExpr"] = parseTrueExpr


def parseUniverseIpSpace(obj):
    return UniverseIpSpace()


aclLineMatchExprParsers["org.batfish.datamodel.UniverseIpSpace"] = parseUniverseIpSpace


class Explanation(namedtuple("Explanation", "positiveHeaderSpace negativeHeaderSpaces origination")):
    __slots__ = ()

    @staticmethod
    def from_conjucts(*conjuncts):
        positiveHeaderSpace = None
        negativeHeaderSpaces = []
        origination = None

        for conj in conjuncts:
            if isinstance(conj, MatchHeaderSpace):
                assert positiveHeaderSpace is None
                positiveHeaderSpace = conj
            elif isinstance(conj, MatchSrcInterface):
                assert origination is None
                origination = conj
            elif isinstance(conj, NotMatchExpr):
                assert isinstance(conj.operand, MatchHeaderSpace)
                negativeHeaderSpaces.append(conj.operand)
            elif isinstance(conj, OriginatingFromDevice):
                assert origination is None
                origination = conj
            elif isinstance(conj, TrueExpr):
                pass
            else:
                raise Exception("unknown conjunct type" + conj)
        return Explanation(positiveHeaderSpace, negativeHeaderSpaces, origination)

    def to_str(self):
        lines = []

        # origination
        origination = self.origination
        if isinstance(origination, OriginatingFromDevice):
            lines.append("Originating from device")
        elif isinstance(origination, MatchSrcInterface):
            ifaces = origination.srcInterfaces
            assert len(ifaces) > 0
            if len(ifaces) == 1:
                lines.append("Originating from interface " + ifaces[0])
            else:
                lines.append("Originating from one interface of: " + ', '.join(ifaces[0]))

        # positive headerspace
        if self.positiveHeaderSpace is not None:
            lines.append("Including " + to_str(self.positiveHeaderSpace))
        else:
            lines.append("Full HeaderSpace")

        # negative headerspaces
        for negativeHeaderSpace in self.negativeHeaderSpaces:
            lines.append("Excluding " + to_str(negativeHeaderSpace))

        return '\n'.join(lines)


headerSpaceFieldNames = {
    'tcpFlagsMatchConditions': 'tcpFlags'
}


def renameHeaderSpaceField(nm):
    return headerSpaceFieldNames.get(nm, nm)


headerSpacePrinters = {}


def to_str(obj):
    if isinstance(obj, str):
        return obj
    elif isinstance(obj, bool):
        return str(obj)
    elif isinstance(obj, list):
        if len(obj) == 1:
            return to_str(obj[0])
        return '[%s]' % (','.join(map(to_str, obj)))
    elif isinstance(obj, dict):
        if 'tcpFlags' in obj:
            return tcp_flags_to_str(obj)
        else:
            return dict_to_str(obj)
    elif obj is None:
        raise Exception("None!")
    elif isinstance(obj, AclIpSpace):
        return obj.simplify().to_str()
    else:
        return obj.to_str()


def tcp_flags_to_str(obj):
    useToFlag = {
        'useAck': 'ack',
        'useCwr': 'cwr',
        'useEce': 'ece',
        'useFin': 'fin',
        'usePsh': 'psh',
        'useRst': 'rst',
        'useSyn': 'syn',
        'useUrg': 'urg'
    }
    use = obj
    val = obj['tcpFlags']
    return dict(
        (key, val[key])
        for (useKey, key) in useToFlag.items()
        if use[useKey])


def dict_to_str(obj):
    return ', '.join('%s=%s' % (key, to_str(val)) for (key, val) in obj.items())


def formatHeaderSpaceExplanation(jsonStr):
    obj = loads(jsonStr)
    ast = parse(obj)

    x = '\n\n'.join((explanation.to_str() for explanation in ast.to_explanations()))
    #print(x)
    return x


