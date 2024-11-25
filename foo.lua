-- put new stuff on term


-- Map gr to use Telescope for references with preview
vim.keymap.set('n', 'gr', '<cmd>Telescope lsp_references<CR>', {
    noremap = true,
    silent = true,
    desc = "Show LSP references with preview"
})

-- Close the quickfix window if it's open
vim.keymap.set('n', '<leader>qc', function()
    vim.cmd('cclose')
end, { noremap = true, desc = "Close quickfix window" })

-- TODO: this shadowed tmux hotkey :/
-- what else should i replace it with?
-- <C-b> close buffer
--vim.keymap.set("n", "<C-b>", function()
--    vim.cmd('bp|bd #')
--end, { noremap = true })

-- fewer buttons to close buffer
vim.keymap.set("n", "<leader>tw", function()
    for _, win in ipairs(vim.api.nvim_list_wins()) do
        local buf = vim.api.nvim_win_get_buf(win)
        if vim.bo[buf].filetype == "trouble" then
            vim.api.nvim_set_current_win(win)
            return
        end
    end
    print(vim.inspect(buffs))
end, { desc = "Focus Trouble window" })


-- WIP working on a way to script setting up a session for running tests
-- like with 2 terminals open to the right place, and a source code file.
-- to do tihs I would need (er want) a idempotent way to open a window & buffer
local function get_or_create_term(name)
    -- Check all buffers for one matching our name
    for _, buf in ipairs(vim.api.nvim_list_bufs()) do
        local buf_name = vim.api.nvim_buf_get_name(buf)
        if buf_name:match(name .. "$") then
            return buf
        end
    end
    -- If we didn't find it, create a new buffer with our name
    local buf = vim.api.nvim_create_buf(true, true)
    vim.api.nvim_set_current_buf(buf)
    vim.api.nvim_buf_set_name(buf, name)
    -- Open terminal in this buffer
    vim.fn.termopen(vim.o.shell)
    return buf
end

vim.api.nvim_create_user_command('GetOrCreateTerm', function(opts)
  local name = opts.args
  get_or_create_term(name)
end, { nargs = 1 })

local function wrap_with_console_log()
  -- Get the visual selection
  local start_line = vim.fn.line("'<")
  local end_line = vim.fn.line("'>")
  local start_col = vim.fn.col("'<")
  local end_col = vim.fn.col("'>")

  -- Get the selected text
  local lines = vim.api.nvim_buf_get_lines(0, start_line - 1, end_line, false)

  -- Handle multi-line selections
  if #lines > 1 then
    -- Adjust first and last lines for partial selections
    lines[1] = string.sub(lines[1], start_col)
    lines[#lines] = string.sub(lines[#lines], 1, end_col)
  else
    -- Handle single line selection
    lines[1] = string.sub(lines[1], start_col, end_col)
  end

  local selected_text = table.concat(lines, '\n')

  -- Get the indentation of the first line
  local indent = vim.fn.indent(start_line)
  local spaces = string.rep(' ', indent)

  -- Create the debug line
  local debug_line = spaces .. 'console.log(" let ' .. selected_text .. ' = ", ' .. selected_text .. ', ";");'

  -- Insert the debug line after the selection
  vim.api.nvim_buf_set_lines(0, end_line, end_line, false, {debug_line})
end

-- Create the command
vim.api.nvim_create_user_command('WrapConsoleLog', function()
  wrap_with_console_log()
end, { range = true })

---- Set up the keybinding (optional)
--vim.keymap.set('v', '<leader>d', ':WrapConsoleLog<CR>', { noremap = true, silent = true })
--
--local M = {}
--function M.format_number(number)
--    local str = tostring(number)
--    -- Get last 2 digits only
--    return str:sub(-1)
--end
--
--function M.get_most_significant(lnum)
--    local str = tostring(lnum)
--    if #str <= 2 then
--        return ""
--    end
--    -- Return all but last 2 digits
--    return str:sub(1, -3)
--end
--function M.format_line(number)
--    local total_lines = vim.fn.line("$")
--    local str = tostring(number)
--
--    -- If less than 1000 lines, just show last 2 digits
--    if total_lines < 1000 then
--        return string.format("%2s", str:sub(-2))  -- pad to 2 chars
--    end
--
--    -- For files with 1000+ lines
--    if number == 1 then
--        -- Show MSD on first line
--        return string.format("%2s", tostring(total_lines):sub(1, -3))
--    else
--        -- Show last 2 digits for other lines
--        return string.format("%2s", str:sub(-2))
--    end
--end
--
--
--
---- Make the functions available globally so statuscolumn can use them
--_G.format_line = M.format_line
--_G.format_number = M.format_number
--_G.get_most_significant = M.get_most_significant
--
---- Set the statuscolumn option to show MSDs at top and last 2 digits per line
--vim.opt.statuscolumn = '%=%{v:lua.format_line(v:lnum)}'
----vim.opt.statuscolumn = '%{v:lua.format_number(v:lnum)}'
