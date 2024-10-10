local array = {}

local function isArray(t)
    if type(t) ~= "table" then
        return false
    end
    local index = 1
    for k, _ in pairs(t) do
        if k ~= index then
            return false
        end
        index = index + 1
    end

    return true
end

array.slice = function(list, startIdx, endIdx)
    if not isArray(list) then
        return {}
    end
    startIdx = (startIdx or 0) +1
    endIdx = (endIdx or #list)
    local result = {}
    for i = startIdx, endIdx do
        if list[i] ~= nil then
            table.insert(result, list[i])
        end
    end
    return result
end

return array