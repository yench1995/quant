from fastapi import APIRouter
from ...strategies.registry import StrategyRegistry
from ...schemas.strategy import StrategySchema, ParameterSpecSchema

router = APIRouter()

@router.get("/strategies", response_model=list[StrategySchema])
async def list_strategies():
    result = []
    for sid, cls in StrategyRegistry.all().items():
        params = [
            ParameterSpecSchema(
                name=p.name,
                type=p.type,
                default=p.default,
                min_val=p.min_val,
                max_val=p.max_val,
                description=p.description,
            )
            for p in cls.PARAMETERS
        ]
        result.append(StrategySchema(
            id=sid,
            name=cls.STRATEGY_NAME,
            description=getattr(cls, "STRATEGY_DESCRIPTION", ""),
            parameters=params,
        ))
    return result

@router.get("/strategies/{strategy_id}", response_model=StrategySchema)
async def get_strategy(strategy_id: str):
    from fastapi import HTTPException
    cls = StrategyRegistry.get(strategy_id)
    if cls is None:
        raise HTTPException(status_code=404, detail="Strategy not found")
    params = [
        ParameterSpecSchema(
            name=p.name,
            type=p.type,
            default=p.default,
            min_val=p.min_val,
            max_val=p.max_val,
            description=p.description,
        )
        for p in cls.PARAMETERS
    ]
    return StrategySchema(
        id=strategy_id,
        name=cls.STRATEGY_NAME,
        description=getattr(cls, "STRATEGY_DESCRIPTION", ""),
        parameters=params,
    )
