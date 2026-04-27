import { useTheme } from "@/context/ThemeContext"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { Palette } from "lucide-react"

export function ShadcnThemeSection() {
  const { theme, setTheme } = useTheme()

  return (
    <Card>
      <CardHeader>
        <CardTitle className="flex items-center gap-2">
          <Palette className="h-5 w-5" />
          Theme Selection
        </CardTitle>
        <CardDescription>
          Choose your preferred interface style.
        </CardDescription>
      </CardHeader>
      <CardContent>
        <div className="flex flex-col space-y-4">
          <div className="grid w-full max-w-sm items-center gap-1.5">
            <Select value={theme} onValueChange={setTheme}>
              <SelectTrigger className="w-full">
                <SelectValue placeholder="Select a theme" />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="qbl-theme">QuickBit (QBL)</SelectItem>
                <SelectItem value="dark">Dark Mode</SelectItem>
                <SelectItem value="light">Light Mode</SelectItem>
                <SelectItem value="my-theme">My Theme</SelectItem>
              </SelectContent>
            </Select>
          </div>
          <p className="text-xs text-muted-foreground">
            Switching themes will immediately update the colors and styles of the entire application.
          </p>
        </div>
      </CardContent>
    </Card>
  )
}
