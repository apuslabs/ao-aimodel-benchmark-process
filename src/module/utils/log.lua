local datetime = require("module.utils.datetime")

local log = {}

local Colors = {
    gray = "\27[90m",
    reset = "\27[0m",
}

log.level = "trace"

local modes = {
    { name = "trace", color = "\27[34m", },
    { name = "debug", color = "\27[36m", },
    { name = "info",  color = "\27[32m", },
    { name = "warn",  color = "\27[33m", },
    { name = "error", color = "\27[31m", },
    { name = "fatal", color = "\27[35m", },
}


local levels = {}
for i, v in ipairs(modes) do
    levels[v.name] = i
end

local _tostring = tostring

local tostring = function(...)
    local t = {}
    for i = 1, select('#', ...) do
        local x = select(i, ...)
        t[#t + 1] = _tostring(x)
    end
    return table.concat(t, " ")
end

for i, x in ipairs(modes) do
    local nameupper = x.name:upper()
    log[x.name] = function(...)
        -- Return early if we're below the log level
        if i < levels[log.level] then
            return
        end

        local msg = tostring(...)

        -- Output to console
        print(string.format("%s%s %s%-7s %s%s",
            Colors.gray,
            datetime.nowISO(),
            x.color,
            "[" .. nameupper .. "]",
            Colors.reset,
            msg)
        )
    end
end

return log
