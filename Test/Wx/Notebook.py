import wx
import wx.lib.agw.flatnotebook as fnb

########################################################################
class TabPanel(wx.Panel):
    """
    This will be the first notebook tab
    """
    #----------------------------------------------------------------------
    def __init__(self, parent):
        """"""

        wx.Panel.__init__(self, parent=parent, id=wx.ID_ANY)

        sizer = wx.BoxSizer(wx.VERTICAL)
        txtOne = wx.TextCtrl(self, wx.ID_ANY, "")
        txtTwo = wx.TextCtrl(self, wx.ID_ANY, "")

        sizer = wx.BoxSizer(wx.VERTICAL)
        sizer.Add(txtOne, 0, wx.ALL, 5)
        sizer.Add(txtTwo, 0, wx.ALL, 5)

        self.SetSizer(sizer)

########################################################################
class FlatNotebookDemo(fnb.FlatNotebook):
    """
    Flatnotebook class
    """

    #----------------------------------------------------------------------
    def __init__(self, parent):
        """Constructor"""
        fnb.FlatNotebook.__init__(self, parent, wx.ID_ANY)

        pageOne = TabPanel(self)
        pageTwo = TabPanel(self)
        pageThree = TabPanel(self)

        self.AddPage(pageOne, "PageOne")
        self.AddPage(pageTwo, "PageTwo")
        self.AddPage(pageThree, "PageThree")

########################################################################
class DemoFrame(wx.Frame):
    """
    Frame that holds all other widgets
    """

    #----------------------------------------------------------------------
    def __init__(self):
        """Constructor"""
        wx.Frame.__init__(self, None, wx.ID_ANY,
                          "FlatNotebook Tutorial",
                          size=(600,400)
                          )
        panel = wx.Panel(self)

        notebook = FlatNotebookDemo(panel)
        sizer = wx.BoxSizer(wx.VERTICAL)
        sizer.Add(notebook, 1, wx.ALL|wx.EXPAND, 5)
        panel.SetSizer(sizer)
        self.Layout()

        self.Show()

#----------------------------------------------------------------------
if __name__ == "__main__":
    app = wx.PySimpleApp()
    frame = DemoFrame()
    app.MainLoop()
