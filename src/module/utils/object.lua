local object = {}

object.getKeys = function(obj)
    if type(obj) ~= "table" then
        return {}
    end
    local res = {}
    for k, _ in pairs(obj) do
        table.insert(res, k)
    end

    return res
end

return object